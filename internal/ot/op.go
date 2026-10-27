// Package ot 實作純文字的 Operational Transformation。
//
// 一個 Op 從文件開頭走到結尾，依序由 retain（保留）、insert（插入）、delete（刪除）組成，
// 涵蓋編輯前的每一個字元。這樣一次編輯可以同時包含多段插入與刪除，
// 連續的按鍵也能 compose 成一個 Op 再送出。長度一律以 rune 計算。
package ot

import (
	"errors"
	"fmt"
)

var (
	ErrLengthMismatch = errors.New("operation length mismatch")
	ErrOutOfRange     = errors.New("position out of range")
)

type kind uint8

const (
	retain kind = iota + 1
	insert
	del
)

type component struct {
	kind kind
	n    int
	text []rune // 只有 insert 使用，n == len(text)
}

type Op struct {
	comps     []component
	baseLen   int
	targetLen int
}

// BaseLen 是套用前的文件長度，TargetLen 是套用後的長度。
func (o Op) BaseLen() int   { return o.baseLen }
func (o Op) TargetLen() int { return o.targetLen }

func (o *Op) Retain(n int) *Op {
	if n <= 0 {
		return o
	}
	o.baseLen += n
	o.targetLen += n
	if l := len(o.comps); l > 0 && o.comps[l-1].kind == retain {
		o.comps[l-1].n += n
		return o
	}
	o.comps = append(o.comps, component{kind: retain, n: n})
	return o
}

func (o *Op) Insert(s string) *Op {
	return o.insertRunes([]rune(s))
}

func (o *Op) insertRunes(r []rune) *Op {
	if len(r) == 0 {
		return o
	}
	o.targetLen += len(r)
	l := len(o.comps)
	switch {
	case l > 0 && o.comps[l-1].kind == insert:
		o.comps[l-1].text = append(o.comps[l-1].text, r...)
		o.comps[l-1].n += len(r)
	case l > 0 && o.comps[l-1].kind == del:
		// 相鄰的 insert 一律排在 delete 前面，同一個編輯只有一種表示法。
		if l > 1 && o.comps[l-2].kind == insert {
			o.comps[l-2].text = append(o.comps[l-2].text, r...)
			o.comps[l-2].n += len(r)
		} else {
			last := o.comps[l-1]
			o.comps = append(o.comps[:l-1], component{kind: insert, n: len(r), text: append([]rune(nil), r...)}, last)
		}
	default:
		o.comps = append(o.comps, component{kind: insert, n: len(r), text: append([]rune(nil), r...)})
	}
	return o
}

func (o *Op) Delete(n int) *Op {
	if n <= 0 {
		return o
	}
	o.baseLen += n
	if l := len(o.comps); l > 0 && o.comps[l-1].kind == del {
		o.comps[l-1].n += n
		return o
	}
	o.comps = append(o.comps, component{kind: del, n: n})
	return o
}

// NewInsert 建立「在長度 docLen 的文件第 pos 個字元前插入 text」的 Op。
func NewInsert(docLen, pos int, text string) (Op, error) {
	if pos < 0 || pos > docLen {
		return Op{}, fmt.Errorf("%w: insert at %d in a document of %d", ErrOutOfRange, pos, docLen)
	}
	var o Op
	o.Retain(pos).Insert(text).Retain(docLen - pos)
	return o, nil
}

// NewDelete 建立「刪除第 pos 個字元起 n 個字元」的 Op。
func NewDelete(docLen, pos, n int) (Op, error) {
	if pos < 0 || n < 0 || pos+n > docLen {
		return Op{}, fmt.Errorf("%w: delete [%d, %d) in a document of %d", ErrOutOfRange, pos, pos+n, docLen)
	}
	var o Op
	o.Retain(pos).Delete(n).Retain(docLen - pos - n)
	return o, nil
}

func (o Op) Apply(doc []rune) ([]rune, error) {
	if len(doc) != o.baseLen {
		return nil, fmt.Errorf("%w: op expects %d runes, document has %d", ErrLengthMismatch, o.baseLen, len(doc))
	}
	out := make([]rune, 0, o.targetLen)
	i := 0
	for _, c := range o.comps {
		switch c.kind {
		case retain:
			out = append(out, doc[i:i+c.n]...)
			i += c.n
		case insert:
			out = append(out, c.text...)
		case del:
			i += c.n
		}
	}
	return out, nil
}

func (o Op) ApplyString(doc string) (string, error) {
	out, err := o.Apply([]rune(doc))
	return string(out), err
}

// InsertedText 回傳這個 Op 插入的所有文字，依位置串接。
func (o Op) InsertedText() string {
	var out []rune
	for _, c := range o.comps {
		if c.kind == insert {
			out = append(out, c.text...)
		}
	}
	return string(out)
}

// iter 逐一走過 component，可以只吃掉一個 component 的前半段。
type iter struct {
	comps []component
	i     int
	off   int
}

func (it *iter) peek() (component, bool) {
	if it.i >= len(it.comps) {
		return component{}, false
	}
	c := it.comps[it.i]
	if it.off > 0 {
		c.n -= it.off
		if c.kind == insert {
			c.text = c.text[it.off:]
		}
	}
	return c, true
}

func (it *iter) take(n int) {
	it.off += n
	if it.off >= it.comps[it.i].n {
		it.i++
		it.off = 0
	}
}

// Compose 把先套用 a 再套用 b 合併成一個 Op，例如 insert("H") 接著 insert("i") 變成 insert("Hi")。
func Compose(a, b Op) (Op, error) {
	if a.targetLen != b.baseLen {
		return Op{}, fmt.Errorf("%w: compose %d->%d with %d->%d", ErrLengthMismatch, a.baseLen, a.targetLen, b.baseLen, b.targetLen)
	}
	var out Op
	ia, ib := iter{comps: a.comps}, iter{comps: b.comps}
	for {
		ca, okA := ia.peek()
		cb, okB := ib.peek()
		if !okA && !okB {
			return out, nil
		}
		// a 刪掉的字元 b 看不到，b 插入的字元 a 沒有，兩者都直接輸出。
		if okA && ca.kind == del {
			out.Delete(ca.n)
			ia.take(ca.n)
			continue
		}
		if okB && cb.kind == insert {
			out.insertRunes(cb.text)
			ib.take(cb.n)
			continue
		}
		if !okA || !okB {
			return Op{}, ErrLengthMismatch
		}
		n := min(ca.n, cb.n)
		switch {
		case ca.kind == retain && cb.kind == retain:
			out.Retain(n)
		case ca.kind == insert && cb.kind == retain:
			out.insertRunes(ca.text[:n])
		case ca.kind == retain && cb.kind == del:
			out.Delete(n)
		}
		// a 插入、b 又刪掉的字元互相抵銷，什麼都不輸出。
		ia.take(n)
		ib.take(n)
	}
}

// Transform 處理兩個基於同一份文件、彼此不知道對方的編輯。against 是 server 已經定序的那一個。
// 回傳 op' 與 against'，滿足 apply(apply(doc, against), op') == apply(apply(doc, op), against')。
//
// 兩邊在同一個位置插入時，against 的文字排在前面：先被定序的編輯先佔位置，
// server 與每個 client 用同一個規則，最後的結果才會一致。
func Transform(op, against Op) (Op, Op, error) {
	if op.baseLen != against.baseLen {
		return Op{}, Op{}, fmt.Errorf("%w: transform ops based on %d and %d runes", ErrLengthMismatch, op.baseLen, against.baseLen)
	}
	var opP, againstP Op
	io, ia := iter{comps: op.comps}, iter{comps: against.comps}
	for {
		co, okO := io.peek()
		ca, okA := ia.peek()
		if !okO && !okA {
			return opP, againstP, nil
		}
		if okA && ca.kind == insert {
			opP.Retain(ca.n)
			againstP.insertRunes(ca.text)
			ia.take(ca.n)
			continue
		}
		if okO && co.kind == insert {
			opP.insertRunes(co.text)
			againstP.Retain(co.n)
			io.take(co.n)
			continue
		}
		if !okO || !okA {
			return Op{}, Op{}, ErrLengthMismatch
		}
		n := min(co.n, ca.n)
		switch {
		case co.kind == retain && ca.kind == retain:
			opP.Retain(n)
			againstP.Retain(n)
		case co.kind == del && ca.kind == retain:
			opP.Delete(n)
		case co.kind == retain && ca.kind == del:
			againstP.Delete(n)
		}
		// 兩邊都刪掉同一段：對方已經刪了，自己不用再刪。
		io.take(n)
		ia.take(n)
	}
}

// TransformIndex 把游標位置 pos 移到套用 o 之後的文件上。
// 插入點剛好在游標上時，pushOnTie 決定游標要不要跟著往後移：游標主人自己打的字會把游標推到後面。
func TransformIndex(pos int, o Op, pushOnTie bool) int {
	out, base := pos, 0
	for _, c := range o.comps {
		if base > pos {
			break
		}
		switch c.kind {
		case retain:
			base += c.n
		case insert:
			if base < pos || pushOnTie {
				out += c.n
			}
		case del:
			out -= min(c.n, pos-base)
			base += c.n
		}
	}
	return out
}
