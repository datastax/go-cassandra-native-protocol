package frame

import (
	"io"
)

type BodyCompressor interface {

	// Algorithm should return the algorithm of this compressor. Currently only LZ4 and SNAPPY are supported.
	Algorithm() string

	// Compress compresses the source, reading it fully, and writes the compressed result to dest.
	Compress(source io.Reader, dest io.Writer) error

	// Decompress decompresses the source, reading it fully, and writes the decompressed result to dest.
	Decompress(source io.Reader, dest io.Writer) error
}
