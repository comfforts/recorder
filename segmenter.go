package recorder

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"

	"github.com/comfforts/errors"
	api "github.com/comfforts/recorder/api/v1"
	"google.golang.org/protobuf/proto"
)

const (
	ERROR_OPENING_FILER      string = "error opening filer %s"
	ERROR_OPENING_INDEX      string = "error opening index %s"
	ERROR_REMOVING_FILER     string = "error removing filer %s"
	ERROR_REMOVING_INDEX     string = "error removing index %s"
	ERROR_MARSHALLING_RECORD string = "error marshalling record"
)

type Segmenter interface {
	Append(record *api.Record) (offset uint64, err error)
	Read(off uint64) (*api.Record, error)
	BaseOffset() uint64
	NextOffset() uint64
	Filer() Filer
	IsMaxed() bool
	Close() error
	Remove() error
	Closed() bool
}

type segmenter struct {
	filer                  Filer
	indexer                Indexer
	baseOffset, nextOffset uint64
	config                 Config
	closed                 bool
}

func newSegmenter(dir string, baseOffset uint64, c Config) (*segmenter, error) {
	s := &segmenter{
		baseOffset: baseOffset,
		config:     c,
	}

	fPath := path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".filer"))
	var filerFile *os.File
	_, err := os.Stat(fPath)
	if err != nil {
		filerFile, err = os.Create(fPath)
	} else {
		filerFile, err = os.Open(fPath)
		log.Printf("segmenter.newSegmenter() - opened existing filer file: %s", fPath)
	}
	if err != nil {
		log.Printf("segmenter.newSegmenter() - error initializing filer file, err: %v", err)
		return nil, errors.WrapError(err, ERROR_OPENING_FILER, fPath)
	}

	if s.filer, err = newFiler(filerFile); err != nil {
		log.Printf("segmenter.newSegmenter() - error creating filer, err: %v", err)
		return nil, err
	}

	var indexFile *os.File
	iPath := path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index"))
	_, err = os.Stat(iPath)
	if err != nil {
		indexFile, err = os.Create(iPath)
	} else {
		indexFile, err = os.Open(iPath)
		log.Printf("segmenter.newSegmenter() - opened existing indexer file: %s", iPath)
	}
	if err != nil {
		log.Printf("segmenter.newSegmenter() - error initializing indexer file, err: %v", err)
		return nil, errors.WrapError(err, ERROR_OPENING_INDEX, iPath)
	}

	if s.indexer, err = newIndexer(indexFile, c); err != nil {
		log.Printf("segmenter.newSegmenter() - error creating indexer, err: %v", err)
		return nil, err
	}
	log.Printf("segmenter.newSegmenter() - indexer size: %d", s.indexer.Size())
	if off, _, err := s.indexer.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

func (s *segmenter) Append(record *api.Record) (offset uint64, err error) {
	if s.IsMaxed() {
		log.Printf("segmenter.Append() - segment is maxed out, baseoffset: %d, nextoffset: %d, indexer size: %d", s.baseOffset, s.nextOffset, s.indexer.Size())
		return 0, io.EOF
	}

	cur := s.nextOffset
	record.Offset = cur

	p, err := proto.Marshal(record)
	if err != nil {
		log.Printf("segmenter.Append() - error marshalling record, error: %v", err)
		return 0, errors.WrapError(err, ERROR_MARSHALLING_RECORD)
	}

	_, pos, err := s.filer.Append(p)
	if err != nil {
		log.Printf("segmenter.Append() - error appending record to filer, error: %v", err)
		return 0, err
	}
	if err = s.indexer.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		log.Printf("segmenter.Append() - error indexing, error: %v", err)
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

func (s *segmenter) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.indexer.Read(int64(off - s.baseOffset))
	if err != nil {
		log.Printf("segmenter.Read() - error reading index, error: %v", err)
		return nil, err
	}

	if s.closed {
		file, err := os.Open(s.filer.Name())
		if err != nil {
			log.Printf("segmenter.Read() - error opening existing filer file: %s, error: %v", s.filer.Name(), err)
			return nil, err
		}

		s.filer, err = newFiler(file)
		if err != nil {
			log.Printf("segmenter.Read() - error creating filer with existing file: %s, error: %v", s.filer.Name(), err)
			return nil, err
		}
		log.Printf("segmenter.Read() - reopened closed filer")
	}
	log.Printf("segmenter.Read() - reading position: %d", pos)
	p, err := s.filer.Read(pos)
	if err != nil {
		log.Printf("segmenter.Read() - error reading position from filer, error: %v", err)
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	if err != nil {
		log.Printf("segmenter.Read() - error unmarshalling record, error: %v", err)
	}
	return record, err
}

func (s *segmenter) IsMaxed() bool {
	return s.indexer.Size() >= s.config.Segment.MaxIndexSize
}

func (s *segmenter) Close() error {
	log.Printf("segmenter.Close() - closing segmenter - offset - base: %d, next: %d", s.baseOffset, s.nextOffset)
	if err := s.indexer.Close(); err != nil {
		log.Printf("segmenter.Close() - error closing indexer, error: %v", err)
		return err
	}
	if err := s.filer.Close(); err != nil {
		log.Printf("segmenter.Close() - error closing filer, error: %v", err)
		return err
	}
	s.closed = true
	return nil
}

func (s *segmenter) Remove() error {
	if !s.Closed() {
		if err := s.Close(); err != nil {
			log.Printf("segmenter.Remove() - error removing segmenter")
			return err
		}
	}
	if err := os.Remove(s.indexer.Name()); err != nil {
		log.Printf("segmenter.Remove() - error removing segmenter indexer file")
		return errors.WrapError(err, ERROR_REMOVING_INDEX, s.indexer.Name())
	}
	if err := os.Remove(s.filer.Name()); err != nil {
		log.Printf("segmenter.Remove() - error removing segmenter filer file")
		return errors.WrapError(err, ERROR_REMOVING_FILER, s.filer.Name())
	}
	return nil
}

func (s *segmenter) Closed() bool {
	return s.closed
}

func (s *segmenter) BaseOffset() uint64 {
	return s.baseOffset
}

func (s *segmenter) NextOffset() uint64 {
	return s.nextOffset
}

func (s *segmenter) Filer() Filer {
	return s.filer
}
