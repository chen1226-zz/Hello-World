package wire

// fieldPriority is the optional field added in protocol v2.
const fieldPriority uint64 = 3

// DefaultPriority is the value a v2 decoder fills in when the peer
// (a v1 sender) did not transmit the optional field.
const DefaultPriority uint64 = 1

// V2 is the v1 layout plus one optional field.
type V2 struct {
	ID       uint64
	Name     string
	Priority uint64

	// Unknown holds fields written by a newer peer, see V1.Unknown.
	Unknown []Field
}

// Encode serializes m as a version-2 frame. The v1 fields keep their
// exact v1 byte layout; the new field is appended after them.
func (m V2) Encode() []byte {
	var body []byte
	body = appendVarintField(body, fieldID, m.ID)
	body = appendStringField(body, fieldName, m.Name)
	body = appendVarintField(body, fieldPriority, m.Priority)
	return encodeFrame(Version2, body)
}

// DecodeV2 parses a frame of any known peer version. When the frame
// comes from a v1 peer the optional field is absent and Priority falls
// back to DefaultPriority.
func DecodeV2(data []byte) (V2, error) {
	_, fields, err := parse(data)
	if err != nil {
		return V2{}, err
	}
	m := V2{Priority: DefaultPriority}
	for _, f := range fields {
		switch {
		case f.ID == fieldID && f.WireType == WireVarint:
			m.ID = f.Uvarint
		case f.ID == fieldName && f.WireType == WireString:
			m.Name = string(f.Bytes)
		case f.ID == fieldPriority && f.WireType == WireVarint:
			m.Priority = f.Uvarint
		default:
			m.Unknown = append(m.Unknown, f)
		}
	}
	return m, nil
}
