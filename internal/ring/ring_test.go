package ring

import (
	"fmt"
	"testing"
)

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("doc_%d", i)
	}
	return out
}

func TestKeysAreSpreadEvenly(t *testing.T) {
	r := New([]string{"edit-0", "edit-1", "edit-2", "edit-3"}, 128)
	load := map[string]int{}
	for _, k := range keys(40000) {
		o, _ := r.Owner(k)
		load[o]++
	}
	for node, n := range load {
		if n < 7000 || n > 13000 {
			t.Fatalf("%s owns %d of 40000 keys: %v", node, n, load)
		}
	}
}

func TestAddingANodeOnlyMovesKeysToIt(t *testing.T) {
	before := New([]string{"edit-0", "edit-1", "edit-2"}, 128)
	after := New([]string{"edit-0", "edit-1", "edit-2", "edit-3"}, 128)
	moved := 0
	for _, k := range keys(30000) {
		a, _ := before.Owner(k)
		b, _ := after.Owner(k)
		if a != b {
			if b != "edit-3" {
				t.Fatalf("%s moved from %s to %s, not to the new node", k, a, b)
			}
			moved++
		}
	}
	// 理想值是 1/4；取模分配（hash % N）會讓大約 3/4 的 key 換 owner。
	if moved < 30000/4-1500 || moved > 30000/4+1500 {
		t.Fatalf("%d of 30000 keys moved", moved)
	}
}

func TestEmptyRingHasNoOwner(t *testing.T) {
	if _, ok := New(nil, 128).Owner("doc"); ok {
		t.Fatal("an empty ring returned an owner")
	}
}
