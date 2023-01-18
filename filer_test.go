package recorder

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const TEST_DATA_DIR = "data"

var (
	TEST_RECORD = []byte("hello world")
	// record width is record length + max size of record length header
	TEST_RECORD_WIDTH = uint64(len(TEST_RECORD)) + RECORD_LENGTH_WIDTH
	TEST_RECORDS      = [][]byte{
		[]byte("hello world"),
		[]byte("hello ninpoop"),
		[]byte("noshua shindam gobba gobba"),
		[]byte("foot fata flaxy"),
	}
)

func TestFilerAppendRead(t *testing.T) {
	fPath := filepath.Join(TEST_DATA_DIR, "append-read-test")
	err := createDirectory(fPath)
	require.NoError(t, err)

	fi, err := os.Create(fPath)
	require.NoError(t, err)

	f1, err := newFiler(fi)
	require.NoError(t, err)

	testAppend(t, f1)
	testRead(t, f1)
	testReadAt(t, f1)

	f2, err := newFiler(fi)
	require.NoError(t, err)
	testRead(t, f2)

	err = f2.Close()
	require.NoError(t, err)

	err = os.RemoveAll(TEST_DATA_DIR)
	require.NoError(t, err)
}

func TestWriteAndRead(t *testing.T) {
	fPath := filepath.Join(TEST_DATA_DIR, "variable-append-read-test")
	err := createDirectory(fPath)
	require.NoError(t, err)

	f, err := os.Create(fPath)
	require.NoError(t, err)

	filer, err := newFiler(f)
	require.NoError(t, err)

	positions := []uint64{}
	bytesWritten := []uint64{}
	for _, v := range TEST_RECORDS {
		rec_width := uint64(len(v)) + RECORD_LENGTH_WIDTH
		n, pos, err := filer.Append(v)
		require.NoError(t, err)
		positions = append(positions, pos)
		bytesWritten = append(bytesWritten, n)
		require.Equal(t, rec_width, n)
	}

	for i, pos := range positions {
		b, err := filer.Read(pos)
		require.NoError(t, err)
		require.Equal(t, string(TEST_RECORDS[i]), string(b))
		require.Equal(t, bytesWritten[i], uint64(RECORD_LENGTH_WIDTH+len(b)))
	}

}

func TestFilerClose(t *testing.T) {
	fPath := filepath.Join(TEST_DATA_DIR, "close-test")
	err := createDirectory(fPath)
	require.NoError(t, err)

	f, err := os.Create(fPath)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	filer, err := newFiler(f)
	require.NoError(t, err)
	_, pos, err := filer.Append(TEST_RECORD)
	require.NoError(t, err)

	err = filer.Close()
	require.NoError(t, err)

	f, afterSize, err := openFile(f.Name())
	require.True(t, afterSize > beforeSize)
	require.NoError(t, err)

	filer, err = newFiler(f)
	require.NoError(t, err)
	b, err := filer.Read(pos)
	require.NoError(t, err)
	require.Equal(t, string(TEST_RECORD), string(b))

	err = filer.Close()
	require.NoError(t, err)

	err = os.RemoveAll(TEST_DATA_DIR)
	require.NoError(t, err)
}

func testAppend(t *testing.T, filer *filer) {
	t.Helper()
	positions := []uint64{}
	for i := uint64(1); i < 4; i++ {
		n, pos, err := filer.Append(TEST_RECORD)
		t.Logf("appended %s, bytes: %d, position: %d", string(TEST_RECORD), n, pos)
		positions = append(positions, pos)
		require.NoError(t, err)
		// position + bytes written should equal record width
		require.Equal(t, pos+n, TEST_RECORD_WIDTH*i)
	}

	t.Logf("positions: %v", positions)
	for _, v := range positions {
		b, err := filer.Read(v)
		require.NoError(t, err)
		t.Logf("read: %s", string(b))
		require.Equal(t, string(TEST_RECORD), string(b))
	}
}

func testRead(t *testing.T, filer *filer) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := filer.Read(pos)
		require.NoError(t, err)
		require.Equal(t, TEST_RECORD, read)
		pos += TEST_RECORD_WIDTH
	}
}

func testReadAt(t *testing.T, filer *filer) {
	t.Helper()
	for i, off := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, RECORD_LENGTH_WIDTH)
		n, err := filer.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, RECORD_LENGTH_WIDTH, n)
		off += int64(n)

		size := ENCODING.Uint64(b)
		b = make([]byte, size)
		n, err = filer.ReadAt(b, off)
		require.NoError(t, err)
		require.Equal(t, TEST_RECORD, b)
		require.Equal(t, int(size), n)
		off += int64(n)
	}
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func createDirectory(path string) error {
	_, err := os.Stat(filepath.Dir(path))
	if err != nil {
		if os.IsNotExist(err) {
			err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
			if err == nil {
				return nil
			}
		}
		return err
	}
	return nil
}
