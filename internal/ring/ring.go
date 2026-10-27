// Package ring 實作 consistent hashing：每個 node 在環上放多個虛擬節點，
// key 由順時針遇到的第一個虛擬節點負責。加入或移除一個 node 時，只有落在它附近的 key 會換 owner。
package ring

import (
	"cmp"
	"hash/fnv"
	"slices"
	"strconv"
)

type Ring struct {
	points []uint64
	owners []string
}

// New 建立環。gateway 與每個 node 拿到同一份 node 清單時，算出的 owner 必定一致。
func New(nodes []string, vnodes int) *Ring {
	type point struct {
		hash  uint64
		owner string
	}
	pts := make([]point, 0, len(nodes)*vnodes)
	for _, n := range nodes {
		for i := range vnodes {
			pts = append(pts, point{hash: Hash(n + "#" + strconv.Itoa(i)), owner: n})
		}
	}
	slices.SortFunc(pts, func(a, b point) int {
		if c := cmp.Compare(a.hash, b.hash); c != 0 {
			return c
		}
		return cmp.Compare(a.owner, b.owner)
	})
	r := &Ring{points: make([]uint64, len(pts)), owners: make([]string, len(pts))}
	for i, p := range pts {
		r.points[i], r.owners[i] = p.hash, p.owner
	}
	return r
}

func (r *Ring) Owner(key string) (string, bool) {
	if r == nil || len(r.points) == 0 {
		return "", false
	}
	h := Hash(key)
	i, _ := slices.BinarySearch(r.points, h)
	if i == len(r.points) {
		i = 0
	}
	return r.owners[i], true
}

// Hash 是 FNV-1a 再加上 murmur3 的 finalizer：FNV 對只差最後幾個字元的 key 分布不夠均勻。
func Hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}
