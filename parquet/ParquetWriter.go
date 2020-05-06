package parquet

import (
	"github.com/json-iterator/go"
	"io"
)

type ParquetWriter struct {
	io.Closer
	schema          *Schema
	writer          io.WriteCloser
	PageSize        int
	meta            *Metadata
	currentRowGroup *RowGroupWriter //当前的rowGroup,只保存一个,完成一个就写入一个,释放一个
	rows            int64
}

var PARK_FLAG = []byte("PAR1")

func NewParquetWriter(schema *Schema, writer io.WriteCloser, pageSize int) *ParquetWriter {
	meta := New(schema.PFields...)
	_, err := writer.Write(PARK_FLAG) //先写入parquet文件开头的标识
	if err != nil {
		return nil
	}
	return &ParquetWriter{
		writer:          writer,
		schema:          schema,
		PageSize:        pageSize,
		meta:            meta,
		currentRowGroup: NewRowGroupWriter(schema, meta, writer, pageSize),
	}
}
func (p *ParquetWriter) WriteJson(json []byte) {
	record := p.schema.GetJsonMap()
	jsoniter.Unmarshal(json, record)
	p.Write(record)
	p.schema.ReturnJsonMap(record)
}

// write record
func (p *ParquetWriter) Write(record *map[string]interface{}) {
	group := p.currentRowGroup
	group.WriteRecord(record)
	p.rows++
	if group.len == p.PageSize {
		p.currentRowGroup.Close()
		//p.currentRowGroup = NewRowGroupWriter(p.schema, p.meta, p.writer, p.PageSize)
		p.meta.StartRowGroup(p.schema.PFields...)
	}
}

func (p *ParquetWriter) Rows() int64 {
	return p.rows
}

func (p *ParquetWriter) Close() error {
	if p.currentRowGroup.len > 0 {
		p.currentRowGroup.Close()
	}
	if err := p.meta.Footer(p.writer); err != nil {
		return err
	}
	_, err := p.writer.Write(PARK_FLAG)
	return err
}
