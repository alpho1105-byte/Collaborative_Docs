package ot

import (
	"encoding/json"
	"errors"
	"math"
)

var errBadComponent = errors.New("op component must be a non-zero integer or a non-empty string")

// MarshalJSON 輸出精簡格式：正整數是 retain、負整數是 delete、字串是 insert，
// 例如 [13,"ument"] 是「保留 13 個字元後插入 ument」。每個按鍵都要上線，格式越小越好。
func (o Op) MarshalJSON() ([]byte, error) {
	out := make([]any, len(o.comps))
	for i, c := range o.comps {
		switch c.kind {
		case retain:
			out[i] = c.n
		case insert:
			out[i] = string(c.text)
		case del:
			out[i] = -c.n
		}
	}
	return json.Marshal(out)
}

func (o *Op) UnmarshalJSON(b []byte) error {
	var raw []json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	var op Op
	for _, r := range raw {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			if s == "" {
				return errBadComponent
			}
			op.Insert(s)
			continue
		}
		var f float64
		if err := json.Unmarshal(r, &f); err != nil || f == 0 || f != math.Trunc(f) || math.Abs(f) > math.MaxInt32 {
			return errBadComponent
		}
		if f > 0 {
			op.Retain(int(f))
		} else {
			op.Delete(int(-f))
		}
	}
	*o = op
	return nil
}
