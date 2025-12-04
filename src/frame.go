package lz4

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/pierrec/xxHash/xxHash32"
)

const (
	magic   = 0x184D2204
	endMark = 0x00000000
	flgByte = 0b01100000
	bdType  = 0b01110000
)

func WriteFrameHeader(w io.Writer) error {
	frameHeader := make([]byte, 7)
	binary.LittleEndian.PutUint32(frameHeader[:4], magic)
	frameHeader[4] = flgByte
	frameHeader[5] = bdType
	frameHeader[6] = getHeaderChecksum(frameHeader[4:6])
	if _, err := w.Write(frameHeader); err != nil {
		return err
	}
	return nil
}

func WriteFrameEndMark(w io.Writer) error {
	endMarkBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(endMarkBytes[:4], endMark)
	if _, err := w.Write(endMarkBytes); err != nil {
		return err
	}
	return nil
}

func getHeaderChecksum(frameHeader []byte) byte {
	x := xxHash32.New(0)
	x.Write(frameHeader)
	return byte((x.Sum32() >> 8) & 0xFF)
}

type DecodedFrameHeader struct {
	Magic                 uint32
	Version               uint8
	BlocksIndependentFlag bool
	BlocksChecksumFlag    bool
	ContentSizeFlag       bool
	ContentChecksumFlag   bool
	DictIDFlag            bool
	ContentSize           uint64
	DictID                uint32
	BlockMaxSize          uint32
}

func ReadFrameHeader(r io.Reader) (*DecodedFrameHeader, error) {

	header := make([]byte, 7)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	magicNum := binary.LittleEndian.Uint32(header[:4])
	if magicNum != magic {
		return nil, ErrCorrupted
	}

	flgByte := header[4]
	bdByte := header[5]

	version := (flgByte >> 6) & 0x03
	if version != 1 {
		return nil, errors.New("lz4: invalid version")
	}

	blocksIndependentFlag := (flgByte & 0x20) != 0
	blocksChecksumFlag := (flgByte & 0x10) != 0
	contentSizeFlag := (flgByte & 0x08) != 0
	contentChecksumFlag := (flgByte & 0x04) != 0
	dictIDFlag := (flgByte & 0x01) != 0

	blockMaxSizeSelector := (bdByte >> 4) & 0x0F
	var blockMaxSize uint32
	switch blockMaxSizeSelector {
	case 4:
		blockMaxSize = 64 << 10
	case 5:
		blockMaxSize = 256 << 10
	case 6:
		blockMaxSize = 1 << 20
	case 7:
		blockMaxSize = 4 << 20
	default:
		return nil, errors.New("lz4: invalid block maximum size")
	}

	result := &DecodedFrameHeader{
		Magic:                 magicNum,
		Version:               version,
		BlocksIndependentFlag: blocksIndependentFlag,
		BlocksChecksumFlag:    blocksChecksumFlag,
		ContentSizeFlag:       contentSizeFlag,
		ContentChecksumFlag:   contentChecksumFlag,
		DictIDFlag:            dictIDFlag,
		BlockMaxSize:          blockMaxSize,
	}

	if contentSizeFlag {
		contentSizeBytes := make([]byte, 8)
		if _, err := io.ReadFull(r, contentSizeBytes); err != nil {
			return nil, err
		}
		result.ContentSize = binary.LittleEndian.Uint64(contentSizeBytes)
	}

	if dictIDFlag {
		dictIDBytes := make([]byte, 4)
		if _, err := io.ReadFull(r, dictIDBytes); err != nil {
			return nil, err
		}
		result.DictID = binary.LittleEndian.Uint32(dictIDBytes)
	}

	return result, nil
}
