package hongo_test

import (
	"fmt"
	"testing"

	"github.com/hotmall/hongo"
)

func TestUse(t *testing.T) {
	db := hongo.Use("test")
	if db == nil {
		fmt.Println("nil")
	} else {
		fmt.Println("not nil")
	}
}
