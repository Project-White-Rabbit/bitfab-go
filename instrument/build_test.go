package instrument

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOverlay_CompilesAutomaticSubtreeAndPreservesGoSemantics(t *testing.T) {
	dir := t.TempDir()
	sdk, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	module := "module example.com/automatic\n\ngo 1.25.0\n\nrequire " + SDKImport + " v0.0.0\n\nreplace " + SDKImport + " => " + sdk + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(instrumentedApplication), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main_test.go"), []byte("package main\nimport \"testing\"\nfunc TestGenerated(t *testing.T) { main() }\n"), 0600); err != nil {
		t.Fatal(err)
	}
	nativeSource := "package main\nfunc native(x int) int { ch:=make(chan int,1);go func(){ch<-2*x}();return <-ch }\n"
	if enabled, err := exec.Command("go", "env", "CGO_ENABLED").Output(); err == nil && strings.TrimSpace(string(enabled)) == "1" {
		nativeSource = "package main\n/*\nstatic int twice(int x) { return 2*x; }\n*/\nimport \"C\"\nfunc nativeTask(ch chan int, x C.int) { ch<-int(C.twice(x)) }\nfunc native(x int) int { ch:=make(chan int,1);go C.twice(C.int(x));go nativeTask(ch,C.int(x));return <-ch }\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "native.go"), []byte(nativeSource), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "mod", "tidy")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("tidy: %v\n%s", err, out)
	}
	overlay, cleanup, err := Overlay(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	var mapping struct{ Replace map[string]string }
	data, err := os.ReadFile(overlay)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &mapping); err != nil {
		t.Fatal(err)
	}
	if len(mapping.Replace) != 3 {
		t.Fatalf("replacements=%v", mapping.Replace)
	}
	original, err := os.ReadFile(filepath.Join(dir, "main.go"))
	if err != nil {
		t.Fatal(err)
	}
	if string(original) != instrumentedApplication {
		t.Fatal("original file changed")
	}
	cmd = exec.Command("go", "run", "-race", "-overlay", overlay, ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		for _, file := range mapping.Replace {
			content, _ := os.ReadFile(file)
			t.Log(string(content))
		}
		t.Fatalf("run: %v\n%s", err, out)
	} else if !strings.Contains(string(out), "automatic capture passed") {
		t.Fatalf("output=%s", out)
	}
	cmd = exec.Command("go", "test", "-race", "-overlay", overlay, ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("instrumented test: %v\n%s", err, out)
	}
}

const instrumentedApplication = `package main

import (
 "context"
 "compress/gzip"
 "io"
 "encoding/json"
 "fmt"
 "net/http"
 "net/http/httptest"
 "sync"
 "time"
 bitfab "github.com/Project-White-Rabbit/bitfab-go"
)

type worker[T ~int] struct{ value T }
func (w *worker[T]) compute(x T) (result T) { defer func(){ result++ }(); return w.value+x }
func leaf(ctx context.Context, x int) (int,error) { if bitfab.GetCurrentSpan(ctx)==nil { panic("missing node context") }; return x+1,nil }
func generic[T any](x T) T { return x }
func wrapper(args ...int) int { return generic(args[0]) }
func task(ch chan int,x int) int { ch<-x; return x }
func arguments(ch chan int) (chan int,int) { return ch,11 }
func variadic(ch chan int,x ...int) { ch<-x[0] }
func recovered() (result int) { defer func(){if recover()!=nil {result=7}}();panic("expected") }
func hidden(ctx context.Context) int { value,_:=leaf(ctx,3);return value }
func run(ctx context.Context) (any,error) {
 w:=worker[int]{value:2};if w.compute(3)!=6 {panic("defer changed")}
 if generic(4)!=4 || recovered()!=7 || wrapper(4)!=4 || native(3)!=6 {panic("return semantics changed")}
 if hidden(ctx)!=4 {panic("hidden result changed")}
 ch:=make(chan int,1);x:=5
 go task(ch,x);x=9;if <-ch!=5 {panic("go argument evaluation moved")}
 go task(arguments(ch));if <-ch!=11 {panic("multiple-value go arguments changed")}
 go variadic(ch,6);if <-ch!=6 {panic("variadic call changed")}
 values:=[]int{8};go variadic(ch,values...);if <-ch!=8 {panic("variadic expansion changed")}
 go func(x int){ch<-generic(x)}(10);if <-ch!=10 {panic("closure call changed")}
 return "actual",nil
}
func main() {
 var mu sync.Mutex;var spans []map[string]any
 server:=httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter,r *http.Request){var reader io.Reader=r.Body;if r.Header.Get("Content-Encoding")=="gzip" {gz,err:=gzip.NewReader(r.Body);if err!=nil {panic(err)};defer gz.Close();reader=gz};var envelope map[string]any;_ = json.NewDecoder(reader).Decode(&envelope);mu.Lock();for _,resource:=range envelope["resourceSpans"].([]any) {for _,scope:=range resource.(map[string]any)["scopeSpans"].([]any) {for _,span:=range scope.(map[string]any)["spans"].([]any) {for _,attr:=range span.(map[string]any)["attributes"].([]any) {a:=attr.(map[string]any);if a["key"]=="bitfab.payload" {var payload map[string]any;_ = json.Unmarshal([]byte(a["value"].(map[string]any)["stringValue"].(string)),&payload);if raw,ok:=payload["rawSpan"].(map[string]any);ok {spans=append(spans,raw)}}}}}};mu.Unlock();_,_=w.Write([]byte("{}"))}));defer server.Close()
 client:=bitfab.NewClient("test",bitfab.WithServiceURL(server.URL),bitfab.WithSimulationPlan(false))
 no:=false;if err:=client.Node(hidden,bitfab.NodeOptions{Capture:&no});err!=nil {panic(err)}
 result,err:=client.Trace(context.Background(),"workflow",run,bitfab.TraceOptions{Name:"workflow"})
 if err!=nil||result!="actual" {panic("root changed")}
 deadline:=time.Now().Add(5*time.Second)
 for {if !client.FlushTraces(5*time.Second) {panic("flush failed")};mu.Lock();taskCount:=0;for _,span:=range spans {if span["span_data"].(map[string]any)["name"]=="task" {taskCount++}};mu.Unlock();if taskCount==2||time.Now().After(deadline) {break};time.Sleep(time.Millisecond)}
 mu.Lock();defer mu.Unlock()
 names:=map[string]int{};var rootID any
 for _,span:=range spans {data:=span["span_data"].(map[string]any);name:=data["name"].(string);names[name]++;if name=="workflow" {rootID=span["id"]};if name=="run"||name=="hidden"||name=="wrapper" {panic("extra root or excluded node")}}
 if names["leaf"]!=1||names["task"]!=2||names["generic"]!=3||names["recovered"]!=1 {panic(fmt.Sprint("missing automatic calls: ",names))}
 for _,span:=range spans {if span["span_data"].(map[string]any)["name"]=="leaf"&&span["parent_id"]!=rootID {panic("excluded node did not reparent child")}}
 fmt.Println("automatic capture passed")
}
`
