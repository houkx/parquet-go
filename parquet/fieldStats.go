package parquet

import (
	"bytes"
	"encoding/binary"
	"math"
	"sort"
)

type int32stats struct {
	min int32
	max int32
}

func newInt32stats() *int32stats {
	return &int32stats{
		min: int32(math.MaxInt32),
	}
}

func (i *int32stats) add(val int32) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *int32stats) bytes(val int32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int32stats) NullCount() *int64 {
	return nil
}

func (f *int32stats) DistinctCount() *int64 {
	return nil
}

func (f *int32stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *int32stats) Max() []byte {
	return f.bytes(f.max)
}

type int32optionalStats struct {
	min     int32
	max     int32
	nils    int64
	nonNils int64
	maxDef  uint8
}

func newint32optionalStats(d uint8) *int32optionalStats {
	return &int32optionalStats{
		min:    int32(math.MaxInt32),
		maxDef: d,
	}
}

func (f *int32optionalStats) add(vals []int32, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < f.maxDef {
			f.nils++
		} else {
			val := vals[i]
			i++

			f.nonNils++
			if val < f.min {
				f.min = val
			}
			if val > f.max {
				f.max = val
			}
		}
	}
}

func (f *int32optionalStats) bytes(val int32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int32optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *int32optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *int32optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *int32optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}

type int64stats struct {
	min int64
	max int64
}

func newInt64stats() *int64stats {
	return &int64stats{
		min: int64(math.MaxInt64),
	}
}

func (i *int64stats) add(val int64) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *int64stats) bytes(val int64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int64stats) NullCount() *int64 {
	return nil
}

func (f *int64stats) DistinctCount() *int64 {
	return nil
}

func (f *int64stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *int64stats) Max() []byte {
	return f.bytes(f.max)
}

type int64optionalStats struct {
	min     int64
	max     int64
	nils    int64
	nonNils int64
	maxDef  uint8
}

func newint64optionalStats(d uint8) *int64optionalStats {
	return &int64optionalStats{
		min:    int64(math.MaxInt64),
		maxDef: d,
	}
}

func (f *int64optionalStats) add(vals []int64, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < f.maxDef {
			f.nils++
		} else {
			val := vals[i]
			i++

			f.nonNils++
			if val < f.min {
				f.min = val
			}
			if val > f.max {
				f.max = val
			}
		}
	}
}

func (f *int64optionalStats) bytes(val int64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *int64optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *int64optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *int64optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *int64optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}

type stringOptionalStats struct {
	vals   []string
	min    []byte
	max    []byte
	nils   int64
	maxDef uint8
}

func newStringOptionalStats(d uint8) *stringOptionalStats {
	return &stringOptionalStats{maxDef: d}
}

func (s *stringOptionalStats) add(vals []string, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < s.maxDef {
			s.nils++
		} else {
			s.vals = append(s.vals, vals[i])
			i++
		}
	}
}

func (s *stringOptionalStats) NullCount() *int64 {
	return &s.nils
}

func (s *stringOptionalStats) DistinctCount() *int64 {
	return nil
}

func (s *stringOptionalStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringOptionalStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringOptionalStats) minMax() {
	if len(s.vals) == 0 {
		return
	}

	tmp := make([]string, len(s.vals))
	copy(tmp, s.vals)
	sort.Strings(tmp)
	s.min = []byte(tmp[0])
	s.max = []byte(tmp[len(tmp)-1])
}

type float32stats struct {
	min float32
	max float32
}

func newFloat32stats() *float32stats {
	return &float32stats{
		min: float32(math.MaxFloat32),
	}
}

func (i *float32stats) add(val float32) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *float32stats) bytes(val float32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *float32stats) NullCount() *int64 {
	return nil
}

func (f *float32stats) DistinctCount() *int64 {
	return nil
}

func (f *float32stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *float32stats) Max() []byte {
	return f.bytes(f.max)
}

type float64stats struct {
	min float64
	max float64
}

func newFloat64stats() *float64stats {
	return &float64stats{
		min: float64(math.MaxFloat64),
	}
}

func (i *float64stats) add(val float64) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *float64stats) bytes(val float64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *float64stats) NullCount() *int64 {
	return nil
}

func (f *float64stats) DistinctCount() *int64 {
	return nil
}

func (f *float64stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *float64stats) Max() []byte {
	return f.bytes(f.max)
}

type float32optionalStats struct {
	min     float32
	max     float32
	nils    int64
	nonNils int64
	maxDef  uint8
}

func newfloat32optionalStats(d uint8) *float32optionalStats {
	return &float32optionalStats{
		min:    float32(math.MaxFloat32),
		maxDef: d,
	}
}

func (f *float32optionalStats) add(vals []float32, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < f.maxDef {
			f.nils++
		} else {
			val := vals[i]
			i++

			f.nonNils++
			if val < f.min {
				f.min = val
			}
			if val > f.max {
				f.max = val
			}
		}
	}
}

func (f *float32optionalStats) bytes(val float32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *float32optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *float32optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *float32optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *float32optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}

type boolOptionalStats struct {
	maxDef uint8
	nils   int64
}

func newBoolOptionalStats(d uint8) *boolOptionalStats {
	return &boolOptionalStats{maxDef: d}
}

func (b *boolOptionalStats) add(vals []bool, defs []uint8) {
	for _, def := range defs {
		if def < b.maxDef {
			b.nils++
		}
	}
}

func (b *boolOptionalStats) NullCount() *int64 {
	return &b.nils
}

func (b *boolOptionalStats) DistinctCount() *int64 {
	return nil
}

func (b *boolOptionalStats) Min() []byte {
	return nil
}

func (b *boolOptionalStats) Max() []byte {
	return nil
}

type uint32stats struct {
	min uint32
	max uint32
}

func newUint32stats() *uint32stats {
	return &uint32stats{
		min: uint32(math.MaxUint32),
	}
}

func (i *uint32stats) add(val uint32) {
	if val < i.min {
		i.min = val
	}
	if val > i.max {
		i.max = val
	}
}

func (f *uint32stats) bytes(val uint32) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *uint32stats) NullCount() *int64 {
	return nil
}

func (f *uint32stats) DistinctCount() *int64 {
	return nil
}

func (f *uint32stats) Min() []byte {
	return f.bytes(f.min)
}

func (f *uint32stats) Max() []byte {
	return f.bytes(f.max)
}

type uint64optionalStats struct {
	min     uint64
	max     uint64
	nils    int64
	nonNils int64
	maxDef  uint8
}

func newuint64optionalStats(d uint8) *uint64optionalStats {
	return &uint64optionalStats{
		min:    uint64(math.MaxUint64),
		maxDef: d,
	}
}

func (f *uint64optionalStats) add(vals []uint64, defs []uint8) {
	var i int
	for _, def := range defs {
		if def < f.maxDef {
			f.nils++
		} else {
			val := vals[i]
			i++

			f.nonNils++
			if val < f.min {
				f.min = val
			}
			if val > f.max {
				f.max = val
			}
		}
	}
}

func (f *uint64optionalStats) bytes(val uint64) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, val)
	return buf.Bytes()
}

func (f *uint64optionalStats) NullCount() *int64 {
	return &f.nils
}

func (f *uint64optionalStats) DistinctCount() *int64 {
	return nil
}

func (f *uint64optionalStats) Min() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.min)
}

func (f *uint64optionalStats) Max() []byte {
	if f.nonNils == 0 {
		return nil
	}
	return f.bytes(f.max)
}

type stringStats struct {
	vals []string
	min  []byte
	max  []byte
}

func newStringStats() *stringStats {
	return &stringStats{}
}

func (s *stringStats) add(val string) {
	s.vals = append(s.vals, val)
}

func (s *stringStats) NullCount() *int64 {
	return nil
}

func (s *stringStats) DistinctCount() *int64 {
	return nil
}

func (s *stringStats) Min() []byte {
	if s.min == nil {
		s.minMax()
	}
	return s.min
}

func (s *stringStats) Max() []byte {
	if s.max == nil {
		s.minMax()
	}
	return s.max
}

func (s *stringStats) minMax() {
	if len(s.vals) == 0 {
		return
	}

	tmp := make([]string, len(s.vals))
	copy(tmp, s.vals)
	sort.Strings(tmp)
	s.min = []byte(tmp[0])
	s.max = []byte(tmp[len(tmp)-1])
}

type boolStats struct{}

func newBoolStats() *boolStats             { return &boolStats{} }
func (b *boolStats) NullCount() *int64     { return nil }
func (b *boolStats) DistinctCount() *int64 { return nil }
func (b *boolStats) Min() []byte           { return nil }
func (b *boolStats) Max() []byte           { return nil }

func pint32(i int32) *int32       { return &i }
func puint32(i uint32) *uint32    { return &i }
func pint64(i int64) *int64       { return &i }
func puint64(i uint64) *uint64    { return &i }
func pbool(b bool) *bool          { return &b }
func pstring(s string) *string    { return &s }
func pfloat32(f float32) *float32 { return &f }
func pfloat64(f float64) *float64 { return &f }

// keeps track of the indices of repeated fields
// that have already been handled by a previous field
type indices []int

func (i indices) rep(rep uint8) {
	if rep > 0 {
		r := int(rep) - 1
		i[r] = i[r] + 1
		for j := int(rep); j < len(i); j++ {
			i[j] = 0
		}
	}
}

func maxDef(types []int) uint8 {
	var out uint8
	for _, typ := range types {
		if typ > 0 {
			out++
		}
	}
	return out
}
