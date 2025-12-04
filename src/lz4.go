package lz4

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	minMatchLength = 4
	maxMatchLength = 0xFFFF
	maxOffset      = 0xFFFF
	hashLog        = 16
	hashSize       = 1 << hashLog
	hashShift      = 32 - hashLog

	defaultBlockSize = 4 * 1024 * 1024
	maxBlockSize     = 4 * 1024 * 1024
)

var (
	ErrBlockTooLarge = errors.New("block size too large")
	ErrCorrupted     = errors.New("corrupted input")
)

type Writer struct {
	dst           io.Writer
	blockSize     int
	hashTable     []uint32
	headerWritten bool
}

type Reader struct {
	src         io.Reader
	blockSize   int
	buffer      []byte
	leftover    []byte
	leftoverPos int
	eof         bool
	headerRead  bool
}

func hashSequence(seq uint32) uint32 {
	return (seq * 2654435761) >> hashShift
}

func NewWriter(dst io.Writer) *Writer {
	return &Writer{
		dst:           dst,
		blockSize:     defaultBlockSize,
		hashTable:     make([]uint32, hashSize),
		headerWritten: false,
	}
}

func compressBlock(src, dst []byte, hashTable []uint32) (int, error) {
	srcLen := len(src)
	if srcLen == 0 {
		return 0, nil
	}

	for i := range hashTable {
		hashTable[i] = 0xFFFFFFFF
	}

	dstPos := 0
	anchor := 0
	srcPos := 0

	for srcPos <= srcLen-minMatchLength {
		seq := binary.LittleEndian.Uint32(src[srcPos:])
		h := hashSequence(seq) & (hashSize - 1)
		ref := hashTable[h]
		hashTable[h] = uint32(srcPos)

		if ref == 0xFFFFFFFF || uint32(srcPos)-ref > maxOffset {
			srcPos++
			continue
		}

		matchLen := 0
		maxLen := srcLen - srcPos
		if maxLen > maxMatchLength {
			maxLen = maxMatchLength
		}
		for matchLen < maxLen && src[srcPos+matchLen] == src[int(ref)+matchLen] {
			matchLen++
		}

		if matchLen < minMatchLength {
			srcPos++
			continue
		}

		literalLen := srcPos - anchor
		token := byte(0)
		if literalLen < 15 {
			token = byte(literalLen << 4)
		} else {
			token = 0xF0
		}

		matchLenCode := matchLen - minMatchLength
		if matchLenCode < 15 {
			token |= byte(matchLenCode)
		} else {
			token |= 0x0F
		}

		if dstPos+1+literalLen+2 > len(dst) {
			return 0, ErrBlockTooLarge
		}

		dst[dstPos] = token
		dstPos++

		if literalLen >= 15 {
			remaining := literalLen - 15
			for remaining >= 255 {
				dst[dstPos] = 255
				dstPos++
				remaining -= 255
			}
			dst[dstPos] = byte(remaining)
			dstPos++
		}

		if literalLen > 0 {
			copy(dst[dstPos:], src[anchor:srcPos])
			dstPos += literalLen
		}

		offset := srcPos - int(ref)
		dst[dstPos] = byte(offset)
		dst[dstPos+1] = byte(offset >> 8)
		dstPos += 2

		if matchLenCode >= 15 {
			remaining := matchLenCode - 15
			for remaining >= 255 {
				dst[dstPos] = 255
				dstPos++
				remaining -= 255
			}
			dst[dstPos] = byte(remaining)
			dstPos++
		}

		srcPos += matchLen
		anchor = srcPos
	}

	if anchor < srcLen {
		literalLen := srcLen - anchor
		token := byte(0)
		if literalLen < 15 {
			token = byte(literalLen << 4)
		} else {
			token = 0xF0
		}

		if dstPos+1+literalLen > len(dst) {
			return 0, ErrBlockTooLarge
		}

		dst[dstPos] = token
		dstPos++

		if literalLen >= 15 {
			remaining := literalLen - 15
			for remaining >= 255 {
				dst[dstPos] = 255
				dstPos++
				remaining -= 255
			}
			dst[dstPos] = byte(remaining)
			dstPos++
		}

		copy(dst[dstPos:], src[anchor:srcLen])
		dstPos += literalLen
	}

	return dstPos, nil
}

func (w *Writer) Write(p []byte) (int, error) {

	if !w.headerWritten {
		if err := WriteFrameHeader(w.dst); err != nil {
			return 0, err
		}
		w.headerWritten = true
	}

	totalWritten := 0
	for len(p) > 0 {
		chunkSize := w.blockSize
		if chunkSize > len(p) {
			chunkSize = len(p)
		}

		worstCaseSize := chunkSize + (chunkSize / 255) + 16
		compressed := make([]byte, worstCaseSize)
		n, err := compressBlock(p[:chunkSize], compressed, w.hashTable)
		if err != nil {
			return totalWritten, err
		}

		var sizeBuf [4]byte
		binary.LittleEndian.PutUint32(sizeBuf[:], uint32(n))
		if _, err := w.dst.Write(sizeBuf[:]); err != nil {
			return totalWritten, err
		}

		if _, err := w.dst.Write(compressed[:n]); err != nil {
			return totalWritten, err
		}

		totalWritten += chunkSize
		p = p[chunkSize:]
	}

	return totalWritten, nil
}

func (w *Writer) Close() error {

	if !w.headerWritten {
		if err := WriteFrameHeader(w.dst); err != nil {
			return err
		}
		w.headerWritten = true
	}

	return WriteFrameEndMark(w.dst)
}

func NewReader(src io.Reader) *Reader {
	return &Reader{
		src:        src,
		blockSize:  defaultBlockSize,
		buffer:     make([]byte, maxBlockSize),
		headerRead: false,
	}
}

func decompressBlock(src, dst []byte) (int, error) {
	srcLen := len(src)
	dstLen := len(dst)
	srcPos := 0
	dstPos := 0

	for srcPos < srcLen {
		if dstPos >= dstLen {
			return dstPos, ErrBlockTooLarge
		}

		token := src[srcPos]
		srcPos++

		litLen := int(token >> 4)
		if litLen == 15 {
			for {
				if srcPos >= srcLen {
					return dstPos, io.ErrUnexpectedEOF
				}
				b := src[srcPos]
				srcPos++
				litLen += int(b)
				if b != 255 {
					break
				}
			}
		}

		if srcPos+litLen > srcLen {
			return dstPos, io.ErrUnexpectedEOF
		}
		if dstPos+litLen > dstLen {
			return dstPos, ErrBlockTooLarge
		}
		if litLen > 0 {
			copy(dst[dstPos:], src[srcPos:srcPos+litLen])
			dstPos += litLen
			srcPos += litLen
		}

		if srcPos >= srcLen {
			break
		}

		if srcPos+2 > srcLen {
			return dstPos, io.ErrUnexpectedEOF
		}
		offset := int(binary.LittleEndian.Uint16(src[srcPos:]))
		srcPos += 2
		if offset == 0 || offset > dstPos {
			return dstPos, ErrCorrupted
		}

		matchLen := int(token & 0x0F)
		if matchLen == 15 {
			for {
				if srcPos >= srcLen {
					return dstPos, io.ErrUnexpectedEOF
				}
				b := src[srcPos]
				srcPos++
				matchLen += int(b)
				if b != 255 {
					break
				}
			}
		}
		matchLen += minMatchLength

		if dstPos+matchLen > dstLen {
			return dstPos, ErrBlockTooLarge
		}
		ref := dstPos - offset
		if ref < 0 {
			return dstPos, ErrCorrupted
		}

		if offset >= matchLen {
			copy(dst[dstPos:], dst[ref:ref+matchLen])
			dstPos += matchLen
		} else {

			for i := 0; i < matchLen; i++ {
				dst[dstPos] = dst[ref+i]
				dstPos++
			}
		}
	}

	return dstPos, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	if r.eof {
		return 0, io.EOF
	}

	if !r.headerRead {
		header, err := ReadFrameHeader(r.src)
		if err != nil {
			return 0, err
		}
		if header.Version != 1 {
			return 0, errors.New("invalid LZ4 version")
		}
		if !header.BlocksIndependentFlag {
			return 0, errors.New("blocks independent flag must be enabled")
		}
		if header.BlocksChecksumFlag {
			return 0, errors.New("blocks checksum flag is not supported")
		}
		if header.ContentSizeFlag {
			return 0, errors.New("content size flag is not supported")
		}

		if header.DictIDFlag {
			return 0, errors.New("dict ID flag is not supported")
		}
		if header.ContentSize > 0 {
			return 0, errors.New("content size is not supported")
		}
		if header.DictID > 0 {
			return 0, errors.New("dict ID is not supported")
		}
		r.headerRead = true
	}

	totalRead := 0

	if r.leftover != nil && r.leftoverPos < len(r.leftover) {
		toCopy := len(r.leftover) - r.leftoverPos
		if toCopy > len(p) {
			toCopy = len(p)
		}
		copy(p[:toCopy], r.leftover[r.leftoverPos:r.leftoverPos+toCopy])
		totalRead += toCopy
		r.leftoverPos += toCopy

		if r.leftoverPos >= len(r.leftover) {
			r.leftover = nil
			r.leftoverPos = 0
		}

		if totalRead >= len(p) {
			return totalRead, nil
		}
	}

	for totalRead < len(p) && !r.eof {

		var sizeBuf [4]byte
		if _, err := io.ReadFull(r.src, sizeBuf[:]); err != nil {
			if err == io.EOF && totalRead > 0 {
				break
			}
			return totalRead, err
		}

		compressedSize := binary.LittleEndian.Uint32(sizeBuf[:])

		if compressedSize == 0 {
			r.eof = true
			break
		}

		uncompressed := (compressedSize & 0x80000000) != 0
		if uncompressed {
			compressedSize &^= 0x80000000
		}

		if compressedSize > uint32(len(r.buffer)) {
			return totalRead, ErrBlockTooLarge
		}

		if _, err := io.ReadFull(r.src, r.buffer[:compressedSize]); err != nil {
			return totalRead, err
		}

		var data []byte
		if uncompressed {

			data = r.buffer[:compressedSize]
		} else {

			decompressed := make([]byte, r.blockSize)
			n, err := decompressBlock(r.buffer[:compressedSize], decompressed)
			if err != nil {
				return totalRead, err
			}
			data = decompressed[:n]
		}

		toCopy := len(data)
		remaining := len(p) - totalRead
		if toCopy > remaining {
			toCopy = remaining

			r.leftover = data[toCopy:]
			r.leftoverPos = 0
		}

		copy(p[totalRead:totalRead+toCopy], data[:toCopy])
		totalRead += toCopy

		if toCopy < len(data) {

			break
		}
	}

	return totalRead, nil
}

func CompressStream(src io.Reader, dst io.Writer) error {
	w := NewWriter(dst)
	defer w.Close()

	buf := make([]byte, 64*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, err := w.Write(buf[:n]); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func DecompressStream(src io.Reader, dst io.Writer) error {
	r := NewReader(src)
	buf := make([]byte, 64*1024)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			if _, err := dst.Write(buf[:n]); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}
