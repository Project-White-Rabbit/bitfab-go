package bitfab

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"unicode"
)

const cycleCheckAfterLevels = 1000

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	isZeroerType      = reflect.TypeFor[interface{ IsZero() bool }]()
	jsonNumberType    = reflect.TypeFor[json.Number]()
)

type sharedRef struct {
	ptr    uintptr
	length int
}

type serializedSizeWalk struct {
	budget    int
	total     int
	level     int
	ancestors map[sharedRef]struct{}
}

type sizeFunc func(w serializedSizeWalk, v reflect.Value) int

type sizedField struct {
	index     []int
	nameLen   int
	omitEmpty bool
	omitZero  bool
	fixed     int
	plainText bool
	size      sizeFunc
}

var (
	sizeFuncsByType sync.Map
	buildSizeFunc   func(reflect.Type) sizeFunc
)

type sizeUnknownError struct {
	t reflect.Type
}

func (e sizeUnknownError) Error() string {
	return "bitfab: could not size values of type " + e.t.String()
}

func exceedsSerializedBudget(v any, budget int) (over bool) {
	defer func() {
		if recover() != nil {
			over = false
		}
	}()
	if v == nil {
		return 4 > budget
	}
	value := reflect.ValueOf(v)
	return sizeFuncFor(value.Type())(serializedSizeWalk{budget: budget}, value) > budget
}

func sizeFuncFor(t reflect.Type) sizeFunc {
	if f, ok := sizeFuncsByType.Load(t); ok {
		return f.(sizeFunc)
	}
	var (
		built sizeFunc
		ready sync.WaitGroup
	)
	ready.Add(1)
	forward, loaded := sizeFuncsByType.LoadOrStore(t, sizeFunc(func(w serializedSizeWalk, v reflect.Value) int {
		ready.Wait()
		return built(w, v)
	}))
	if loaded {
		return forward.(sizeFunc)
	}
	defer ready.Done()
	defer func() {
		if failure := recover(); failure != nil {
			built = sizeUnknown(t)
			sizeFuncsByType.Store(t, built)
			panic(failure)
		}
	}()
	build := buildSizeFunc
	if build == nil {
		build = newSizeFunc
	}
	built = build(t)
	sizeFuncsByType.Store(t, built)
	return built
}

func sizeUnknown(t reflect.Type) sizeFunc {
	return func(serializedSizeWalk, reflect.Value) int {
		panic(sizeUnknownError{t: t})
	}
}

func hasCustomEncoding(t reflect.Type) bool {
	if t.Implements(jsonMarshalerType) || t.Implements(textMarshalerType) {
		return true
	}
	if t.Kind() == reflect.Pointer {
		return false
	}
	pointer := reflect.PointerTo(t)
	return pointer.Implements(jsonMarshalerType) || pointer.Implements(textMarshalerType)
}

func isNumberKind(k reflect.Kind) bool {
	switch k {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func newSizeFunc(t reflect.Type) sizeFunc {
	if hasCustomEncoding(t) {
		return sizeOne
	}
	switch k := t.Kind(); {
	case k == reflect.Bool:
		return sizeBool
	case isNumberKind(k):
		return sizeOne
	case k == reflect.String && t == jsonNumberType:
		return sizeJSONNumber
	case k == reflect.String:
		return sizeString
	case k == reflect.Interface:
		return sizeInterface
	case k == reflect.Pointer:
		return newPointerSizeFunc(t)
	case k == reflect.Map:
		return newMapSizeFunc(t)
	case k == reflect.Slice && encodesAsBase64(t):
		return sizeBase64
	case k == reflect.Slice:
		return newSliceSizeFunc(t)
	case k == reflect.Array:
		return newElementsSizeFunc(t.Elem())
	case k == reflect.Struct:
		return newStructSizeFunc(t)
	}
	return sizeNothing
}

func sizeNothing(w serializedSizeWalk, _ reflect.Value) int {
	return w.total
}

func sizeOne(w serializedSizeWalk, _ reflect.Value) int {
	return w.total + 1
}

func sizeBool(w serializedSizeWalk, _ reflect.Value) int {
	return w.total + 4
}

func sizeString(w serializedSizeWalk, v reflect.Value) int {
	return w.total + v.Len() + 2
}

func sizeJSONNumber(w serializedSizeWalk, v reflect.Value) int {
	return w.total + v.Len()
}

func sizeBase64(w serializedSizeWalk, v reflect.Value) int {
	if v.IsNil() {
		return w.total + 4
	}
	return w.total + base64.StdEncoding.EncodedLen(v.Len()) + 2
}

func sizeInterface(w serializedSizeWalk, v reflect.Value) int {
	if v.IsNil() {
		return w.total + 4
	}
	elem := v.Elem()
	return sizeFuncFor(elem.Type())(w, elem)
}

func newPointerSizeFunc(t reflect.Type) sizeFunc {
	elemSize := sizeFuncFor(t.Elem())
	return func(w serializedSizeWalk, v reflect.Value) int {
		if v.IsNil() {
			return w.total + 4
		}
		if w.total > w.budget {
			return w.total
		}
		ref := sharedRef{ptr: v.Pointer()}
		if !w.enter(ref) {
			return w.total
		}
		total := elemSize(w, v.Elem())
		w.leave(ref)
		return total
	}
}

func (w *serializedSizeWalk) enter(ref sharedRef) bool {
	w.level++
	if w.level <= cycleCheckAfterLevels {
		return true
	}
	if w.ancestors == nil {
		w.ancestors = map[sharedRef]struct{}{}
	}
	if _, seen := w.ancestors[ref]; seen {
		return false
	}
	w.ancestors[ref] = struct{}{}
	return true
}

func (w *serializedSizeWalk) leave(ref sharedRef) {
	if w.level > cycleCheckAfterLevels {
		delete(w.ancestors, ref)
	}
}

func encodesAsBase64(t reflect.Type) bool {
	if t.Elem().Kind() != reflect.Uint8 {
		return false
	}
	pointer := reflect.PointerTo(t.Elem())
	return !pointer.Implements(jsonMarshalerType) && !pointer.Implements(textMarshalerType)
}

func newSliceSizeFunc(t reflect.Type) sizeFunc {
	elementsSize := newElementsSizeFunc(t.Elem())
	return func(w serializedSizeWalk, v reflect.Value) int {
		if v.IsNil() {
			return w.total + 4
		}
		ref := sharedRef{ptr: v.Pointer(), length: v.Len()}
		if !w.enter(ref) {
			return w.total
		}
		total := elementsSize(w, v)
		w.leave(ref)
		return total
	}
}

func newElementsSizeFunc(elemType reflect.Type) sizeFunc {
	if each := fixedElementBytes(elemType); each > 0 {
		return func(w serializedSizeWalk, v reflect.Value) int {
			if n := v.Len(); n > 0 {
				return w.total + 1 + n*(each+1)
			}
			return w.total + 2
		}
	}
	if isPlainString(elemType) {
		return func(w serializedSizeWalk, v reflect.Value) int {
			n := v.Len()
			if n == 0 {
				return w.total + 2
			}
			total := w.total + 1 + 3*n
			for i := 0; i < n && total <= w.budget; i++ {
				total += v.Index(i).Len()
			}
			return total
		}
	}
	elemSize := sizeFuncFor(elemType)
	return func(w serializedSizeWalk, v reflect.Value) int {
		n := v.Len()
		if n == 0 {
			return w.total + 2
		}
		w.total += 1 + n
		for i := 0; i < n && w.total <= w.budget; i++ {
			w.total = elemSize(w, v.Index(i))
		}
		return w.total
	}
}

func isPlainString(t reflect.Type) bool {
	return t.Kind() == reflect.String && t != jsonNumberType && !hasCustomEncoding(t)
}

func fixedElementBytes(t reflect.Type) int {
	if hasCustomEncoding(t) {
		return 1
	}
	if t.Kind() == reflect.Bool {
		return 4
	}
	if isNumberKind(t.Kind()) {
		return 1
	}
	return 0
}

func newMapSizeFunc(t reflect.Type) sizeFunc {
	keyBytes := mapKeyBytes(t.Key())
	elemSize := sizeFuncFor(t.Elem())
	return func(w serializedSizeWalk, v reflect.Value) int {
		if v.IsNil() {
			return w.total + 4
		}
		if w.total > w.budget {
			return w.total
		}
		ref := sharedRef{ptr: v.Pointer()}
		if !w.enter(ref) {
			return w.total
		}
		w.total += 2
		var iter reflect.MapIter
		iter.Reset(v)
		var key, elem reflect.Value
		reuse := v.CanInterface()
		if reuse {
			key = reflect.New(t.Key()).Elem()
			elem = reflect.New(t.Elem()).Elem()
		}
		for i := 0; iter.Next() && w.total <= w.budget; i++ {
			if i > 0 {
				w.total++
			}
			if reuse {
				key.SetIterKey(&iter)
				elem.SetIterValue(&iter)
			} else {
				key, elem = iter.Key(), iter.Value()
			}
			w.total += keyBytes(key) + 1
			w.total = elemSize(w, elem)
		}
		w.leave(ref)
		return w.total
	}
}

func mapKeyBytes(t reflect.Type) func(reflect.Value) int {
	switch {
	case t.Kind() == reflect.String:
		return func(k reflect.Value) int { return k.Len() + 2 }
	case t.Implements(textMarshalerType):
		return func(reflect.Value) int { return 2 }
	case isNumberKind(t.Kind()) && t.Kind() != reflect.Float32 && t.Kind() != reflect.Float64:
		return func(reflect.Value) int { return 3 }
	}
	return func(reflect.Value) int { return 0 }
}

func newStructSizeFunc(t reflect.Type) sizeFunc {
	fields := sizedFieldsOf(t)
	return func(w serializedSizeWalk, v reflect.Value) int {
		w.total += 2
		emitted := 0
		for i := range fields {
			field := &fields[i]
			if w.total > w.budget {
				return w.total
			}
			if field.fixed > 0 {
				if emitted > 0 {
					w.total++
				}
				emitted++
				w.total += field.nameLen + 3 + field.fixed
				continue
			}
			fv, ok := fieldAt(v, field.index)
			if !ok {
				continue
			}
			if field.omitEmpty && isEmptyJSONValue(fv) {
				continue
			}
			if field.omitZero && omittedAsZero(fv) {
				continue
			}
			if emitted > 0 {
				w.total++
			}
			emitted++
			w.total += field.nameLen + 3
			if field.plainText {
				w.total += fv.Len() + 2
				continue
			}
			w.total = field.size(w, fv)
		}
		return w.total
	}
}

func fieldAt(v reflect.Value, index []int) (reflect.Value, bool) {
	if len(index) == 1 {
		return v.Field(index[0]), true
	}
	for depth, i := range index {
		if depth > 0 && v.Kind() == reflect.Pointer {
			if v.IsNil() {
				return reflect.Value{}, false
			}
			v = v.Elem()
		}
		v = v.Field(i)
	}
	return v, true
}

func isEmptyJSONValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Pointer:
		return v.IsNil()
	}
	return false
}

type sizedFieldCandidate struct {
	name      string
	fieldType reflect.Type
	field     sizedField
}

func sizedFieldsOf(t reflect.Type) []sizedField {
	var candidates []sizedFieldCandidate
	collectSizedFields(t, nil, map[reflect.Type]bool{}, &candidates)
	counts := map[string]int{}
	for _, c := range candidates {
		counts[c.name]++
	}
	fields := make([]sizedField, 0, len(candidates))
	for _, c := range candidates {
		if counts[c.name] == 1 {
			c.field.size = sizeFuncFor(c.fieldType)
			c.field.plainText = isPlainString(c.fieldType)
			if len(c.field.index) == 1 && !c.field.omitEmpty && !c.field.omitZero {
				c.field.fixed = fixedElementBytes(c.fieldType)
			}
			fields = append(fields, c.field)
		}
	}
	return fields
}

func collectSizedFields(t reflect.Type, prefix []int, onPath map[reflect.Type]bool, out *[]sizedFieldCandidate) {
	if onPath[t] {
		return
	}
	onPath[t] = true
	defer delete(onPath, t)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		fieldType := sf.Type
		if sf.Anonymous {
			if fieldType.Kind() == reflect.Pointer {
				fieldType = fieldType.Elem()
			}
			if !sf.IsExported() && fieldType.Kind() != reflect.Struct {
				continue
			}
		} else if !sf.IsExported() {
			continue
		}
		tag := sf.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if !isValidJSONFieldName(name) {
			name = ""
		}
		index := append(append([]int(nil), prefix...), i)
		if name == "" && sf.Anonymous && fieldType.Kind() == reflect.Struct {
			collectSizedFields(fieldType, index, onPath, out)
			continue
		}
		if name == "" {
			name = sf.Name
		}
		*out = append(*out, sizedFieldCandidate{
			name:      name,
			fieldType: sf.Type,
			field: sizedField{
				index:     index,
				nameLen:   len(name),
				omitEmpty: hasTagOption(options, "omitempty"),
				omitZero:  hasTagOption(options, "omitzero"),
			},
		})
	}
}

func hasTagOption(options, want string) bool {
	for options != "" {
		var option string
		option, options, _ = strings.Cut(options, ",")
		if option == want {
			return true
		}
	}
	return false
}

func isValidJSONFieldName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		switch {
		case strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", c):
		case !unicode.IsLetter(c) && !unicode.IsDigit(c):
			return false
		}
	}
	return true
}

// omittedAsZero reports whether encoding/json leaves out an omitzero field. A
// type's own IsZero method decides when it has one, as in encoding/json. When
// that method cannot be called here (a pointer receiver on a value that is not
// addressable, or a field that cannot be read), the field is treated as left
// out, which only undercounts.
func omittedAsZero(fv reflect.Value) bool {
	if (fv.Kind() == reflect.Pointer || fv.Kind() == reflect.Interface) && fv.IsNil() {
		return true
	}
	if fv.Type().Implements(isZeroerType) {
		if !fv.CanInterface() {
			return true
		}
		return fv.Interface().(interface{ IsZero() bool }).IsZero()
	}
	if reflect.PointerTo(fv.Type()).Implements(isZeroerType) {
		if !fv.CanAddr() || !fv.Addr().CanInterface() {
			return true
		}
		return fv.Addr().Interface().(interface{ IsZero() bool }).IsZero()
	}
	return fv.IsZero()
}
