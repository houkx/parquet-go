package parquet

import (
	"io"
)

type RowGroupWriter struct {
	fieldData []Values
	len       int
	meta      *Metadata
	w         io.Writer
	schema    *Schema
}

func NewRowGroupWriter(schema *Schema, meta *Metadata, w io.Writer, maxRecords int) *RowGroupWriter {
	fieldDatas := make([]Values, len(schema.PFields))
	for i, f := range schema.Fields {
		vs := f.makeValues(maxRecords)
		fieldDatas[i] = *vs
	}
	return &RowGroupWriter{meta: meta,
		w: w, schema: schema,
		fieldData: fieldDatas}
}
func (p *RowGroupWriter) WriteRecord(record *map[string]interface{}) {
	for i, f := range p.schema.Fields {
		f.append(&p.fieldData[i], record)
	}
	p.meta.NextDoc()
	p.len++
}

func (p *RowGroupWriter) Close() (err error) {
	for i, f := range p.schema.Fields {
	  f.write(p.w, p.meta, &p.fieldData[i])
	}
	p.len = 0
	return err
}
