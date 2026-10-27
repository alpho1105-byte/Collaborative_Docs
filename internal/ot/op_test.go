package ot

import (
	"encoding/json"
	"math/rand/v2"
	"testing"
)

func mustApply(t *testing.T, o Op, doc string) string {
	t.Helper()
	out, err := o.ApplyString(doc)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// 講義的例子：A 與 B 都看到 "This is a doc"，A 在結尾插入 "ument"，B 在結尾插入 "!"。
// server 先收到 A，B 的編輯要轉換成 INSERT(18, "!")，兩邊最後都是 "This is a document!"。
func TestConcurrentInsertsAtTheSamePositionConverge(t *testing.T) {
	doc := "This is a doc"
	a, _ := NewInsert(13, 13, "ument")
	b, _ := NewInsert(13, 13, "!")

	bPrime, aPrime, err := Transform(b, a)
	if err != nil {
		t.Fatal(err)
	}
	server := mustApply(t, bPrime, mustApply(t, a, doc))
	clientB := mustApply(t, aPrime, mustApply(t, b, doc))
	if server != "This is a document!" || clientB != server {
		t.Fatalf("server %q, client B %q", server, clientB)
	}
	want, _ := NewInsert(18, 18, "!")
	if got, _ := json.Marshal(bPrime); string(got) != mustJSON(want) {
		t.Fatalf("B' = %s, want INSERT(18, \"!\")", got)
	}
}

func mustJSON(o Op) string {
	b, _ := json.Marshal(o)
	return string(b)
}

func TestTransformHandlesOverlappingDeletesAndInsertsInsideDeletedRanges(t *testing.T) {
	doc := "abcdefgh"
	for _, tc := range []struct {
		name string
		op   func() Op
		ag   func() Op
		want string
	}{
		{"same range deleted twice", func() Op { o, _ := NewDelete(8, 2, 3); return o }, func() Op { o, _ := NewDelete(8, 2, 3); return o }, "abfgh"},
		{"overlapping deletes", func() Op { o, _ := NewDelete(8, 1, 4); return o }, func() Op { o, _ := NewDelete(8, 3, 4); return o }, "ah"},
		{"insert at the start of a deleted range survives", func() Op { o, _ := NewInsert(8, 2, "XY"); return o }, func() Op { o, _ := NewDelete(8, 2, 3); return o }, "abXYfgh"},
		{"insert at the end of a deleted range survives", func() Op { o, _ := NewInsert(8, 5, "XY"); return o }, func() Op { o, _ := NewDelete(8, 2, 3); return o }, "abXYfgh"},
		{"insert inside a deleted range survives", func() Op { o, _ := NewInsert(8, 4, "XY"); return o }, func() Op { o, _ := NewDelete(8, 2, 3); return o }, "abXYfgh"},
	} {
		op, ag := tc.op(), tc.ag()
		opP, agP, err := Transform(op, ag)
		if err != nil {
			t.Fatal(err)
		}
		left := mustApply(t, opP, mustApply(t, ag, doc))
		right := mustApply(t, agP, mustApply(t, op, doc))
		if left != tc.want || right != tc.want {
			t.Errorf("%s: %q / %q, want %q", tc.name, left, right, tc.want)
		}
	}
}

const alphabet = "ab中文«»xyz "

func randomString(r *rand.Rand, n int) string {
	runes := []rune(alphabet)
	out := make([]rune, n)
	for i := range out {
		out[i] = runes[r.IntN(len(runes))]
	}
	return string(out)
}

func randomOp(r *rand.Rand, docLen int) Op {
	var o Op
	for left := docLen; left > 0; {
		n := 1 + r.IntN(min(left, 5))
		switch r.IntN(3) {
		case 0:
			o.Retain(n)
			left -= n
		case 1:
			o.Delete(n)
			left -= n
		case 2:
			o.Insert(randomString(r, 1+r.IntN(4)))
		}
	}
	if r.IntN(2) == 0 {
		o.Insert(randomString(r, 1+r.IntN(3)))
	}
	return o
}

func TestRandomTransformsConvergeAndComposeMatchesSequentialApply(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		doc := randomString(r, r.IntN(20))
		n := len([]rune(doc))
		a, b := randomOp(r, n), randomOp(r, n)

		aP, bP, err := Transform(a, b)
		if err != nil {
			t.Fatal(err)
		}
		left := mustApply(t, aP, mustApply(t, b, doc))
		right := mustApply(t, bP, mustApply(t, a, doc))
		if left != right {
			t.Fatalf("doc %q a %s b %s: %q != %q", doc, mustJSON(a), mustJSON(b), left, right)
		}

		next := randomOp(r, a.TargetLen())
		ab, err := Compose(a, next)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := mustApply(t, ab, doc), mustApply(t, next, mustApply(t, a, doc)); got != want {
			t.Fatalf("compose: %q != %q", got, want)
		}
	}
}

func TestTransformIndex(t *testing.T) {
	ins, _ := NewInsert(10, 4, "abc")
	del, _ := NewDelete(10, 2, 5)
	for _, tc := range []struct {
		name string
		pos  int
		op   Op
		push bool
		want int
	}{
		{"insert before the cursor shifts it", 6, ins, false, 9},
		{"insert after the cursor leaves it", 3, ins, false, 3},
		{"someone else's insert at the cursor stays after it", 4, ins, false, 4},
		{"your own insert at the cursor pushes it", 4, ins, true, 7},
		{"delete before the cursor pulls it back", 9, del, false, 4},
		{"cursor inside a deleted range collapses to its start", 5, del, false, 2},
		{"delete after the cursor leaves it", 1, del, false, 1},
	} {
		if got := TransformIndex(tc.pos, tc.op, tc.push); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestJSONRoundTripAndValidation(t *testing.T) {
	var o Op
	o.Retain(3).Insert("中文").Delete(2).Retain(1)
	b, _ := json.Marshal(o)
	if string(b) != `[3,"中文",-2,1]` {
		t.Fatalf("marshal = %s", b)
	}
	var back Op
	if err := json.Unmarshal(b, &back); err != nil || back.BaseLen() != 6 || back.TargetLen() != 6 {
		t.Fatalf("unmarshal = %+v, %v", back, err)
	}
	for _, bad := range []string{`[0]`, `[""]`, `[1.5]`, `[true]`, `{}`, `[1e12]`} {
		if err := json.Unmarshal([]byte(bad), &back); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
	if _, err := o.ApplyString("short"); err == nil {
		t.Fatal("applying to a document of the wrong length must fail")
	}
}
