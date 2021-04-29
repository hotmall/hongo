package hongo_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hotmall/hongo"
)

func TestFindOne(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filter := `{ "geometry": { "$geoIntersects": { "$geometry": { "type": "Point", "coordinates": [ -73.93414657, 40.82302903 ] } } } }`
	v, err := hongo.Use("test").Collection("neighborhoods").FindOne(ctx, filter)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(v)
}
