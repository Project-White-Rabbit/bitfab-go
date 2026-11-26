package bitfab

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
)

var (
	cloudArrayIndex    = regexp.MustCompile(`\A(?:0|[1-9][0-9]*)\z`)
	cloudDecimalNumber = regexp.MustCompile(`\A[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\z`)
)

type cloudObject struct {
	keys   []string
	values map[string]any
}

type cloudMissing struct{}

func newCloudObject() *cloudObject {
	return &cloudObject{values: map[string]any{}}
}

func (object *cloudObject) lookup(key string) (any, bool) {
	value, ok := object.values[key]
	return value, ok
}

func (object *cloudObject) get(key string) any {
	return object.values[key]
}

func (object *cloudObject) field(key string) any {
	if value, ok := object.values[key]; ok {
		return value
	}
	return cloudMissing{}
}

func (object *cloudObject) set(key string, value any) {
	if _, ok := object.values[key]; !ok {
		object.keys = append(object.keys, key)
	}
	object.values[key] = value
}

func (object *cloudObject) orderedKeys() []string {
	var indexes, named []string
	for _, key := range object.keys {
		if cloudArrayIndex.MatchString(key) {
			if index, err := strconv.ParseUint(key, 10, 64); err == nil && index < math.MaxUint32 {
				indexes = append(indexes, key)
				continue
			}
		}
		named = append(named, key)
	}
	sort.SliceStable(indexes, func(left, right int) bool {
		first, _ := strconv.ParseUint(indexes[left], 10, 64)
		second, _ := strconv.ParseUint(indexes[right], 10, 64)
		return first < second
	})
	return append(indexes, named...)
}

func parseCloudJSON(text string) (any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	value, err := readCloudJSON(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("extra data after the JSON value")
	}
	return value, nil
}

func readCloudJSON(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if number, ok := token.(json.Number); ok {
		parsed, err := strconv.ParseFloat(number.String(), 64)
		var failure *strconv.NumError
		if err != nil && !(errors.As(err, &failure) && failure.Err == strconv.ErrRange) {
			return nil, err
		}
		return parsed, nil
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := newCloudObject()
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("object key is not a string")
			}
			value, err := readCloudJSON(decoder)
			if err != nil {
				return nil, err
			}
			object.set(key, value)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return object, nil
	case '[':
		items := []any{}
		for decoder.More() {
			value, err := readCloudJSON(decoder)
			if err != nil {
				return nil, err
			}
			items = append(items, value)
		}
		if _, err := decoder.Token(); err != nil {
			return nil, err
		}
		return items, nil
	}
	return nil, fmt.Errorf("unexpected %v", delimiter)
}

func cloudJSNumber(number float64) string {
	if math.IsNaN(number) {
		return "NaN"
	}
	if math.IsInf(number, 1) {
		return "Infinity"
	}
	if math.IsInf(number, -1) {
		return "-Infinity"
	}
	if number == 0 {
		return "0"
	}
	sign := ""
	if number < 0 {
		sign = "-"
		number = -number
	}
	scientific := strconv.FormatFloat(number, 'e', -1, 64)
	mantissa, exponentText, _ := strings.Cut(scientific, "e")
	digits := strings.Replace(mantissa, ".", "", 1)
	exponent, _ := strconv.Atoi(exponentText)
	point := exponent + 1
	count := len(digits)
	switch {
	case count <= point && point <= 21:
		return sign + digits + strings.Repeat("0", point-count)
	case 0 < point && point <= 21:
		return sign + digits[:point] + "." + digits[point:]
	case -6 < point && point <= 0:
		return sign + "0." + strings.Repeat("0", -point) + digits
	}
	exponentSign := "+"
	if point-1 < 0 {
		exponentSign = "-"
	}
	magnitude := strconv.Itoa(int(math.Abs(float64(point - 1))))
	if count == 1 {
		return sign + digits + "e" + exponentSign + magnitude
	}
	return sign + digits[:1] + "." + digits[1:] + "e" + exponentSign + magnitude
}

func compactCloudJSON(value any) string {
	var builder strings.Builder
	writeCloudJSON(&builder, value, false)
	return builder.String()
}

func pythonCloudJSON(value any) string {
	var builder strings.Builder
	writeCloudJSON(&builder, value, true)
	return builder.String()
}

func writeCloudJSON(builder *strings.Builder, value any, python bool) {
	itemSeparator, keySeparator := ",", ":"
	if python {
		itemSeparator, keySeparator = ", ", ": "
	}
	switch typed := value.(type) {
	case nil, cloudMissing:
		builder.WriteString("null")
	case bool:
		builder.WriteString(strconv.FormatBool(typed))
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			builder.WriteString("null")
			return
		}
		builder.WriteString(cloudJSNumber(typed))
	case int:
		builder.WriteString(strconv.Itoa(typed))
	case int64:
		builder.WriteString(strconv.FormatInt(typed, 10))
	case string:
		writeCloudString(builder, typed, python)
	case []string:
		builder.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				builder.WriteString(itemSeparator)
			}
			writeCloudString(builder, item, python)
		}
		builder.WriteByte(']')
	case []any:
		builder.WriteByte('[')
		for index, item := range typed {
			if index > 0 {
				builder.WriteString(itemSeparator)
			}
			writeCloudJSON(builder, item, python)
		}
		builder.WriteByte(']')
	case *cloudObject:
		builder.WriteByte('{')
		for index, key := range typed.orderedKeys() {
			if index > 0 {
				builder.WriteString(itemSeparator)
			}
			writeCloudString(builder, key, python)
			builder.WriteString(keySeparator)
			writeCloudJSON(builder, typed.values[key], python)
		}
		builder.WriteByte('}')
	default:
		encoded, err := json.Marshal(typed)
		if err != nil {
			builder.WriteString("null")
			return
		}
		builder.Write(encoded)
	}
}

func writeCloudString(builder *strings.Builder, text string, ascii bool) {
	builder.WriteByte('"')
	for _, character := range text {
		switch character {
		case '"':
			builder.WriteString(`\"`)
		case '\\':
			builder.WriteString(`\\`)
		case '\n':
			builder.WriteString(`\n`)
		case '\r':
			builder.WriteString(`\r`)
		case '\t':
			builder.WriteString(`\t`)
		case '\b':
			builder.WriteString(`\b`)
		case '\f':
			builder.WriteString(`\f`)
		default:
			switch {
			case character < 0x20 || (ascii && character >= 0x7f && character <= 0xffff):
				fmt.Fprintf(builder, `\u%04x`, character)
			case ascii && character > 0xffff:
				first, second := utf16.EncodeRune(character)
				fmt.Fprintf(builder, `\u%04x\u%04x`, first, second)
			default:
				builder.WriteRune(character)
			}
		}
	}
	builder.WriteByte('"')
}

func cloudTruthy(value any) bool {
	switch typed := value.(type) {
	case nil, cloudMissing:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0 && !math.IsNaN(typed)
	case int:
		return typed != 0
	}
	return true
}

func cloudInteger(value any) (float64, bool) {
	number, ok := value.(float64)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) || number != math.Trunc(number) {
		return 0, false
	}
	return number, true
}

func cloudJSString(value any) string {
	switch typed := value.(type) {
	case cloudMissing:
		return "undefined"
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(typed)
	case string:
		return typed
	case float64:
		return cloudJSNumber(typed)
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case []string:
		return strings.Join(typed, ",")
	case []any:
		parts := make([]string, len(typed))
		for index, item := range typed {
			if item != nil {
				parts[index] = cloudJSString(item)
			}
		}
		return strings.Join(parts, ",")
	}
	return "[object Object]"
}

func cloudJSToNumber(value any) float64 {
	switch typed := value.(type) {
	case nil:
		return 0
	case bool:
		if typed {
			return 1
		}
		return 0
	case float64:
		return typed
	case int:
		return float64(typed)
	case string:
		return cloudJSStringToNumber(typed)
	case []any:
		if len(typed) == 0 {
			return 0
		}
		if len(typed) == 1 {
			return cloudJSStringToNumber(cloudJSString(typed))
		}
	}
	return math.NaN()
}

func cloudJSStringToNumber(text string) float64 {
	text = strings.TrimFunc(text, cloudJSWhitespace)
	if text == "" {
		return 0
	}
	switch text {
	case "Infinity", "+Infinity":
		return math.Inf(1)
	case "-Infinity":
		return math.Inf(-1)
	}
	if len(text) > 2 && text[0] == '0' {
		base := map[byte]int{'x': 16, 'X': 16, 'o': 8, 'O': 8, 'b': 2, 'B': 2}[text[1]]
		if base != 0 {
			parsed, err := strconv.ParseUint(text[2:], base, 64)
			if err != nil {
				return math.NaN()
			}
			return float64(parsed)
		}
	}
	if !cloudDecimalNumber.MatchString(text) {
		return math.NaN()
	}
	parsed, err := strconv.ParseFloat(text, 64)
	var failure *strconv.NumError
	if err != nil && !(errors.As(err, &failure) && failure.Err == strconv.ErrRange) {
		return math.NaN()
	}
	return parsed
}

func cloudJSWhitespace(character rune) bool {
	switch character {
	case '\t', '\n', '\v', '\f', '\r', ' ', 0xa0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
		return true
	}
	return character >= 0x2000 && character <= 0x200a
}

func cloudUTF16Length(text string) int {
	length := 0
	for _, character := range text {
		length++
		if character > 0xffff {
			length++
		}
	}
	return length
}

func cloudUTF16Prefix(text string, limit int) string {
	var builder strings.Builder
	length := 0
	for _, character := range text {
		width := 1
		if character > 0xffff {
			width = 2
		}
		if length+width > limit {
			if length < limit {
				builder.WriteRune(utf8.RuneError)
			}
			break
		}
		builder.WriteRune(character)
		length += width
	}
	return builder.String()
}

func pythonCloudRepr(value any) string {
	switch typed := value.(type) {
	case nil:
		return "None"
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(typed, `\`, `\\`), "'", `\'`) + "'"
	}
	return compactCloudJSON(value)
}

func pythonCloudText(value any) string {
	switch typed := value.(type) {
	case bool:
		if typed {
			return "True"
		}
		return "False"
	case string:
		return typed
	}
	return pythonCloudJSON(value)
}

func validCloudText(data []byte) string {
	if utf8.Valid(data) {
		return string(data)
	}
	var builder strings.Builder
	for len(data) > 0 {
		character, size := utf8.DecodeRune(data)
		if character != utf8.RuneError || size > 1 {
			builder.Write(data[:size])
			data = data[size:]
			continue
		}
		builder.WriteRune(utf8.RuneError)
		data = data[cloudIllFormedLength(data):]
	}
	return builder.String()
}

func cloudIllFormedLength(data []byte) int {
	lead := data[0]
	var needed int
	low, high := byte(0x80), byte(0xbf)
	switch {
	case lead >= 0xc2 && lead <= 0xdf:
		needed = 1
	case lead >= 0xe0 && lead <= 0xef:
		needed = 2
		if lead == 0xe0 {
			low = 0xa0
		} else if lead == 0xed {
			high = 0x9f
		}
	case lead >= 0xf0 && lead <= 0xf4:
		needed = 3
		if lead == 0xf0 {
			low = 0x90
		} else if lead == 0xf4 {
			high = 0x8f
		}
	default:
		return 1
	}
	consumed := 1
	for consumed <= needed && consumed < len(data) {
		next := data[consumed]
		if next < low || next > high {
			break
		}
		low, high = 0x80, 0xbf
		consumed++
	}
	return consumed
}
