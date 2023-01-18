package recorder

import (
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileRead(t *testing.T) {
	datas := Mapper{
		0: 0000,
		1: 0001,
		2: 0002,
	}
	fmt.Println("data: ", datas)
	fileName := "test-file"

	fi, err := os.Create(fileName)
	require.NoError(t, err)

	encoder := gob.NewEncoder(fi)
	err = encoder.Encode(&datas)
	require.NoError(t, err)

	err = fi.Close()
	require.NoError(t, err)

	fi, err = os.Open(fileName)
	require.NoError(t, err)

	decoder := gob.NewDecoder(fi)
	var result Mapper
	err = decoder.Decode(&result)
	require.NoError(t, err)

	fmt.Println("result: ", result)

	err = os.Remove(fileName)
	require.NoError(t, err)
}

func TestIndexer(t *testing.T) {
	fPath := filepath.Join(TEST_DATA_DIR, "indexer_test")
	err := createDirectory(fPath)
	require.NoError(t, err)

	f, err := os.Create(fPath)
	require.NoError(t, err)

	c := Config{}

	idx, err := newIndexer(f, c)
	require.NoError(t, err)

	require.Equal(t, uint64(0), idx.size)

	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{Off: 0, Pos: 0},
		{Off: 1, Pos: 10},
	}

	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)

		_, pos, err := idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// indexer should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, ErrRecordPosition, err)
	err = idx.Close()
	require.NoError(t, err)

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndexer(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)

	err = idx.Close()
	require.NoError(t, err)

	err = os.RemoveAll(TEST_DATA_DIR)
	require.NoError(t, err)
}
