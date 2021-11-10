package quartz

import (
	"reflect"
	"testing"
)

func assertEqual(t *testing.T, a interface{}, b interface{}) {
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("actual(%v) != expected(%v)", a, b)
	}
}

func assertEqualInt64(t *testing.T, a int64, b int64) {
	if a != b {
		t.Fatalf("actual(%d) != expected(%d)", a, b)
	}
}

func assertNotEqual(t *testing.T, a interface{}, b interface{}) {
	if reflect.DeepEqual(a, b) {
		t.Fatalf("actual(%v) == expected(%v)", a, b)
	}
}

func TestUtils(t *testing.T) {
	hash := HashCode("foo")
	assertEqual(t, hash, 2851307223)
}
