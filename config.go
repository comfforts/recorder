package recorder

type Config struct {
	Segment struct {
		// MaxIndexSize specifies the maximum number of entries in a segment.
		MaxIndexSize uint64
		// InitialOffset specifies the starting offset
		InitialOffset uint64
	}
}
