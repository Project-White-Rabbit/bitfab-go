package bitfab

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	os.Setenv("BITFAB_DISABLE_SIM_PLAN", "1")
	os.Exit(m.Run())
}
