package parquet

import sh "github.com/houkx/parquet-go/parquet/schema"

func Int32Type(se *sh.SchemaElement) {
	t := sh.Type_INT32
	se.Type = &t
}

func Uint32Type(se *sh.SchemaElement) {
	t := sh.Type_INT32
	se.Type = &t
	ct := sh.ConvertedType_UINT_32
	se.ConvertedType = &ct
}

func Int64Type(se *sh.SchemaElement) {
	t := sh.Type_INT64
	se.Type = &t
}

func Uint64Type(se *sh.SchemaElement) {
	t := sh.Type_INT64
	se.Type = &t
	ct := sh.ConvertedType_UINT_64
	se.ConvertedType = &ct
}

func Float32Type(se *sh.SchemaElement) {
	t := sh.Type_FLOAT
	se.Type = &t
}

func Float64Type(se *sh.SchemaElement) {
	t := sh.Type_DOUBLE
	se.Type = &t
}

func BoolType(se *sh.SchemaElement) {
	t := sh.Type_BOOLEAN
	se.Type = &t
}

func StringType(se *sh.SchemaElement) {
	t := sh.Type_BYTE_ARRAY
	se.Type = &t
}
