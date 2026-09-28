package wire

// Field ids shared by all versions. Ids are append-only: once shipped,
// an id and its wire type never change.
const (
	fieldID   uint64 = 1
	fieldName uint64 = 2
)

// V1 is the original message layout.
type V1 struct {
	ID   uint64
	Name string

	// Unknown holds fields written by a newer peer that this version
	// does not understand. They are skipped on decode and preserved
	// here so a proxy can forward them untouched.
	Unknown []Field
}

// Encode serializes m as a version-1 frame. The byte layout is frozen:
// golden samples under testdata/v1 regress any accidental change.
func (m V1) Encode() []byte {
	var body []byte
	body = appendVarintField(body, fieldID, m.ID)
	body = appendStringField(body, fieldName, m.Name)
	return encodeFrame(Version1, body)
}

// DecodeV1 parses a frame of any known peer version. Fields it does not
// recognize (e.g. the optional field added in v2) are skipped via their
// TLV boundary and retained in Unknown.
func DecodeV1(data []byte) (V1, error) {
	version, fields, err := parse(data)
	if err != nil {
		return V1{}, err
	}
	_ = version
	var m V1
	for _, f := range fields {
		switch {
		case f.ID == fieldID && f.WireType == WireVarint:
			m.ID = f.Uvarint
		case f.ID == fieldName && f.WireType == WireString:
			m.Name = string(f.Bytes)
		default:
			m.Unknown = append(m.Unknown, f)
		}
	}
	return m, nil
}
