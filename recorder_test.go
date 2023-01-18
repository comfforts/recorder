package recorder

import (
	"fmt"
	"io"
	"os"
	"testing"

	api "github.com/comfforts/recorder/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestRecorder(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T, recorder Recorder,
	){
		"append and read a record succeeds": testAppendReadRecorder,
		"offset out of range error":         testOutOfRangeErrRecorder,
		"init with existing segments":       testInitExistingRecorder,
		"reader":                            testReaderRecorder,
		"truncate":                          testTruncateRecorder,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir := fmt.Sprintf("%s/", TEST_DATA_DIR)
			err := createDirectory(dir)
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxIndexSize = 3
			c.Segment.InitialOffset = 0
			recorder, err := NewRecorder(dir, c)
			require.NoError(t, err)

			fn(t, recorder)
		})
	}
}

func testAppendReadRecorder(t *testing.T, recorder Recorder) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := recorder.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := recorder.Read(off)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)

}

func testOutOfRangeErrRecorder(t *testing.T, recorder Recorder) {
	read, err := recorder.Read(1)
	require.Nil(t, read)
	t.Logf("error: %v", err)
}

func testInitExistingRecorder(t *testing.T, recorder Recorder) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 5; i++ {
		_, err := recorder.Append(append)
		require.NoError(t, err)
	}
	require.NoError(t, recorder.Close())

	off, err := recorder.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = recorder.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(4), off)

	n, err := NewRecorder(recorder.Directory(), recorder.Configuration())
	require.NoError(t, err)

	off, err = n.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)
	off, err = n.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(4), off)
}

func testReaderRecorder(t *testing.T, recorder Recorder) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := recorder.Append(append)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := recorder.Reader()
	b, err := io.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[RECORD_LENGTH_WIDTH:], read)
	require.NoError(t, err)
	require.Equal(t, append.Value, read.Value)
}

func testTruncateRecorder(t *testing.T, recorder Recorder) {
	append := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 4; i++ {
		_, err := recorder.Append(append)
		require.NoError(t, err)
	}

	err := recorder.Truncate(2)
	require.NoError(t, err)

	_, err = recorder.Read(0)
	require.Error(t, err)
}
