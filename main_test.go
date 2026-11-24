package bitfab

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("BITFAB_DISABLE_SIM_PLAN", "1")
	home, err := os.MkdirTemp("", "bitfab-go-test-home-")
	if err != nil {
		panic(err)
	}
	os.Setenv("HOME", home)
	code := m.Run()
	os.RemoveAll(home)
	os.Exit(code)
}
