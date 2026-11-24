package typeutils

import "github.com/datazip-inc/olake/types"

func Resolve(stream *types.Stream, objects ...map[string]interface{}) error {
	var sample SchemaSample
	for _, object := range objects {
		sample.Add(object)
	}
	sample.Apply(stream)
	return nil
}

// SchemaSample infers field types across records without keeping the records in memory.
type SchemaSample struct {
	fields Fields
	seen   map[string]int
	count  int
}

func (s *SchemaSample) Add(record map[string]any) {
	if s.fields == nil {
		s.fields = make(Fields)
		s.seen = make(map[string]int)
	}
	s.count++
	for column, value := range record {
		field := NewField(TypeFromValue(value))
		previous, exists := s.fields[column]
		switch {
		case value == nil:
			if !exists {
				s.fields[column] = field
			}
			continue
		case s.seen[column] != 0:
			previous.Merge(field)
		default:
			s.fields[column] = field
		}
		s.seen[column]++
	}
}

func (s *SchemaSample) Apply(stream *types.Stream) {
	for column, field := range s.fields {
		stream.UpsertField(column, field.Type(), s.seen[column] < s.count, false)
	}
}
