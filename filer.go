package recorder

import (
	"bufio"
	"encoding/binary"
	"log"
	"os"
	"sync"

	"github.com/comfforts/errors"
)

var (
	ENCODING = binary.BigEndian
)

// https://go.dev/ref/spec#Size_and_alignment_guarantees
const (
	RECORD_LENGTH_WIDTH = 8
)

const (
	ERROR_NO_FILE        string = "%s doesn't exist"
	ERROR_REC_LEN_APPEND string = "error wrting record length in %s"
	ERROR_REC_APPEND     string = "error appending record in %s"
	ERROR_BUFFER         string = "error flushing buffer for %s"
	ERROR_REC_LEN_READ   string = "error reading record length in %s"
	ERROR_REC_READ       string = "error reading record in %s"
)

type Filer interface {
	Append(record []byte) (n uint64, pos uint64, err error)
	Read(pos uint64) ([]byte, error)
	ReadAt(p []byte, off int64) (int, error)
	Close() error
	Name() string
}

// filer: os.File Wrapper for buffered and indexed read/write
type filer struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newFiler(f *os.File) (*filer, error) {
	fs, err := os.Stat(f.Name())
	if err != nil {
		log.Printf("filer.newFiler() - error getting filer file stats, error: %v", err)
		return nil, errors.WrapError(err, ERROR_NO_FILE, f.Name())
	}
	size := uint64(fs.Size())
	return &filer{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (f *filer) Append(record []byte) (n uint64, pos uint64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// new record position
	pos = f.size

	// append record length
	if err := binary.Write(f.buf, ENCODING, uint64(len(record))); err != nil {
		log.Printf("filer.Append() - error appending record, error: %v", err)
		return 0, 0, errors.WrapError(err, ERROR_REC_LEN_APPEND, f.Name())
	}

	// append record
	w, err := f.buf.Write(record)
	if err != nil {
		log.Printf("filer.Append() - error writing buffer, error: %v", err)
		return 0, 0, errors.WrapError(err, ERROR_REC_APPEND, f.Name())
	}

	// final record size
	w += RECORD_LENGTH_WIDTH

	// update file size
	f.size += uint64(w)

	return uint64(w), pos, nil
}

func (f *filer) Read(pos uint64) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// flush buffer for any unwritten record
	if err := f.buf.Flush(); err != nil {
		log.Printf("filer.Read() - error flushing file buffer, err: %v", err)
		return nil, errors.WrapError(err, ERROR_BUFFER, f.Name())
	}

	// read record length
	size := make([]byte, RECORD_LENGTH_WIDTH)
	if _, err := f.File.ReadAt(size, int64(pos)); err != nil {
		log.Printf("filer.Read() - error reading record length, error: %v", err)
		return nil, errors.WrapError(err, ERROR_REC_LEN_READ, f.Name())
	}

	// read record
	b := make([]byte, ENCODING.Uint64(size))
	if _, err := f.File.ReadAt(b, int64(pos+RECORD_LENGTH_WIDTH)); err != nil {
		log.Printf("filer.Read() - error reading record, error: %v", err)
		return nil, errors.WrapError(err, ERROR_REC_READ, f.Name())
	}
	return b, nil
}

func (f *filer) ReadAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.buf.Flush(); err != nil {
		log.Printf("filer.Read() - error flushing buffer, error: %v", err)
		return 0, errors.WrapError(err, ERROR_BUFFER, f.Name())
	}
	return f.File.ReadAt(p, off)
}

func (f *filer) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.buf.Flush()
	if err != nil {
		log.Printf("filer.Close() - error flushing buffer, error: %v", err)
		return errors.WrapError(err, ERROR_BUFFER, f.Name())
	}
	return f.File.Close()
}
