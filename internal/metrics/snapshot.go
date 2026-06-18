package metrics

import (
	"fmt"
	"math"

	"google.golang.org/protobuf/encoding/protowire"
)

// The metrics snapshot wire format (ADR-0035): a versioned, hand-written
// protobuf — the same codec discipline as the metadata records (ADR-0023), so
// the CLI and the web console (v0.11) decode one typed model rather than
// re-parsing Prometheus text. Additive: fields are only ever added.
//
//	message Snapshot { repeated Family families = 1; }
//	message Family {
//	  string name = 1; string help = 2; string type = 3;
//	  repeated string label_names = 4; repeated Sample samples = 5;
//	}
//	message Sample { repeated string labels = 1; double value = 2; }

// MarshalSnapshot encodes families to the wire snapshot.
func MarshalSnapshot(families []Family) []byte {
	var b []byte
	for _, f := range families {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendBytes(b, marshalFamily(f))
	}
	return b
}

func marshalFamily(f Family) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, f.Name)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, f.Help)
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendString(b, f.Type)
	for _, ln := range f.LabelNames {
		b = protowire.AppendTag(b, 4, protowire.BytesType)
		b = protowire.AppendString(b, ln)
	}
	for _, s := range f.Samples {
		b = protowire.AppendTag(b, 5, protowire.BytesType)
		b = protowire.AppendBytes(b, marshalSample(s))
	}
	return b
}

func marshalSample(s Sample) []byte {
	var b []byte
	for _, l := range s.Labels {
		b = protowire.AppendTag(b, 1, protowire.BytesType)
		b = protowire.AppendString(b, l)
	}
	b = protowire.AppendTag(b, 2, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(s.Value))
	return b
}

// UnmarshalSnapshot decodes a wire snapshot. Unknown fields are skipped
// (additive evolution).
func UnmarshalSnapshot(b []byte) ([]Family, error) {
	var families []Family
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("metrics: snapshot tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		if num == 1 && typ == protowire.BytesType {
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, fmt.Errorf("metrics: family bytes: %w", protowire.ParseError(n))
			}
			b = b[n:]
			f, err := unmarshalFamily(v)
			if err != nil {
				return nil, err
			}
			families = append(families, f)
			continue
		}
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return nil, fmt.Errorf("metrics: snapshot skip: %w", protowire.ParseError(n))
		}
		b = b[n:]
	}
	return families, nil
}

func unmarshalFamily(b []byte) (Family, error) {
	var f Family
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return f, fmt.Errorf("metrics: family tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return f, fmt.Errorf("metrics: family name: %w", protowire.ParseError(n))
			}
			f.Name, b = v, b[n:]
		case num == 2 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return f, fmt.Errorf("metrics: family help: %w", protowire.ParseError(n))
			}
			f.Help, b = v, b[n:]
		case num == 3 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return f, fmt.Errorf("metrics: family type: %w", protowire.ParseError(n))
			}
			f.Type, b = v, b[n:]
		case num == 4 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return f, fmt.Errorf("metrics: family label name: %w", protowire.ParseError(n))
			}
			f.LabelNames, b = append(f.LabelNames, v), b[n:]
		case num == 5 && typ == protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return f, fmt.Errorf("metrics: family sample: %w", protowire.ParseError(n))
			}
			b = b[n:]
			s, err := unmarshalSample(v)
			if err != nil {
				return f, err
			}
			f.Samples = append(f.Samples, s)
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return f, fmt.Errorf("metrics: family skip: %w", protowire.ParseError(n))
			}
			b = b[n:]
		}
	}
	return f, nil
}

func unmarshalSample(b []byte) (Sample, error) {
	var s Sample
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return s, fmt.Errorf("metrics: sample tag: %w", protowire.ParseError(n))
		}
		b = b[n:]
		switch {
		case num == 1 && typ == protowire.BytesType:
			v, n := protowire.ConsumeString(b)
			if n < 0 {
				return s, fmt.Errorf("metrics: sample label: %w", protowire.ParseError(n))
			}
			s.Labels, b = append(s.Labels, v), b[n:]
		case num == 2 && typ == protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(b)
			if n < 0 {
				return s, fmt.Errorf("metrics: sample value: %w", protowire.ParseError(n))
			}
			s.Value, b = math.Float64frombits(v), b[n:]
		default:
			n = protowire.ConsumeFieldValue(num, typ, b)
			if n < 0 {
				return s, fmt.Errorf("metrics: sample skip: %w", protowire.ParseError(n))
			}
			b = b[n:]
		}
	}
	return s, nil
}
