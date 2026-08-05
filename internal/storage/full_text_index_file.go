package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/marcisbee/flop/internal/util"
)

var fullTextIndexMagic = [4]byte{'F', 'T', 'I', 'X'}

const fullTextIndexFileVersion uint16 = 1

type fullTextTokenSnapshot struct {
	id   uint32
	text string
}

// MarshalFullTextIndex returns a deterministic, checksummed snapshot of index.
// Postings are reconstructed from document terms when loading, which keeps the
// durable representation compact and avoids persisting the same relationship
// twice.
func MarshalFullTextIndex(index *FullTextIndex, checkpointLSN uint64) ([]byte, error) {
	if index == nil {
		return nil, fmt.Errorf("full-text index is nil")
	}

	index.mu.RLock()
	defer index.mu.RUnlock()

	tokens, docSlots, err := fullTextSnapshotLayout(index)
	if err != nil {
		return nil, err
	}

	encodedSize := 18 + 4 + 4
	for _, token := range tokens {
		encodedSize += 8 + len(token.text)
	}
	for i := 0; i < docSlots; i++ {
		pkLen := 0
		if i < len(index.docPKs) {
			pkLen = len(index.docPKs[i])
		}
		termCount := 0
		if i < len(index.docTerms) {
			termCount = len(index.docTerms[i])
		}
		encodedSize += 10 + pkLen + 4*termCount
	}
	var payload bytes.Buffer
	payload.Grow(encodedSize)
	_, _ = payload.Write(make([]byte, 18))
	if err := writeFullTextIndexPayload(&payload, index, tokens, docSlots); err != nil {
		return nil, err
	}

	out := payload.Bytes()
	copy(out[:4], fullTextIndexMagic[:])
	binary.LittleEndian.PutUint16(out[4:6], fullTextIndexFileVersion)
	binary.LittleEndian.PutUint32(out[6:10], util.CRC32(out[18:]))
	binary.LittleEndian.PutUint64(out[10:18], checkpointLSN)
	return out, nil
}

// WriteFullTextIndexFile durably replaces path with a full-text index snapshot.
func WriteFullTextIndexFile(path string, index *FullTextIndex, checkpointLSN uint64) error {
	if index == nil {
		return fmt.Errorf("full-text index is nil")
	}
	return writeFileAtomicFunc(path, 0o644, func(file *os.File) error {
		index.mu.RLock()
		defer index.mu.RUnlock()

		tokens, docSlots, err := fullTextSnapshotLayout(index)
		if err != nil {
			return err
		}
		header := make([]byte, 18)
		copy(header[:4], fullTextIndexMagic[:])
		binary.LittleEndian.PutUint16(header[4:6], fullTextIndexFileVersion)
		binary.LittleEndian.PutUint64(header[10:18], checkpointLSN)
		if _, err := file.Write(header); err != nil {
			return err
		}

		checksummed := &fullTextChecksumWriter{writer: file}
		buffered := bufio.NewWriterSize(checksummed, 256<<10)
		if err := writeFullTextIndexPayload(buffered, index, tokens, docSlots); err != nil {
			return err
		}
		if err := buffered.Flush(); err != nil {
			return err
		}
		if _, err := file.Seek(6, io.SeekStart); err != nil {
			return err
		}
		var encoded [4]byte
		binary.LittleEndian.PutUint32(encoded[:], checksummed.checksum)
		_, err = file.Write(encoded[:])
		return err
	})
}

// WriteFullTextIndexData durably writes a snapshot returned by
// MarshalFullTextIndex.
func WriteFullTextIndexData(path string, data []byte) error {
	return writeFileAtomic(path, data, 0o644)
}

// ReadFullTextIndexFile loads a durable full-text index snapshot.
func ReadFullTextIndexFile(path string) (*FullTextIndex, uint64, error) {
	data, release, err := readIndexFileBytes(path)
	if err != nil {
		return nil, 0, err
	}
	defer release()
	return parseFullTextIndex(data)
}

func parseFullTextIndex(data []byte) (*FullTextIndex, uint64, error) {
	if len(data) < 18 {
		return nil, 0, fmt.Errorf("invalid full-text index: truncated header")
	}
	if !bytes.Equal(data[:4], fullTextIndexMagic[:]) {
		return nil, 0, fmt.Errorf("invalid full-text index: bad magic")
	}
	if version := binary.LittleEndian.Uint16(data[4:6]); version != fullTextIndexFileVersion {
		return nil, 0, fmt.Errorf("unsupported full-text index version: %d", version)
	}
	checkpointLSN := binary.LittleEndian.Uint64(data[10:18])
	payload := data[18:]
	if got, want := util.CRC32(payload), binary.LittleEndian.Uint32(data[6:10]); got != want {
		return nil, 0, fmt.Errorf("invalid full-text index: checksum mismatch")
	}

	reader := bytes.NewReader(payload)
	tokenCount, err := readBoundedCount(reader, "token", 8)
	if err != nil {
		return nil, 0, err
	}
	index := NewFullTextIndex()
	maxTokenID := uint32(0)
	for i := 0; i < tokenCount; i++ {
		tokenID, err := readUint32(reader)
		// MarshalFullTextIndex emits the interned token IDs in their original,
		// contiguous order. Requiring that shape rejects duplicate IDs and keeps
		// document term references cheap to validate below.
		if err != nil || tokenID != uint32(i+1) {
			return nil, 0, fmt.Errorf("invalid full-text index token id at %d", i)
		}
		token, err := readSizedString(reader)
		if err != nil || token == "" {
			return nil, 0, fmt.Errorf("invalid full-text index token at %d", i)
		}
		if _, duplicate := index.tokenIDByText[token]; duplicate {
			return nil, 0, fmt.Errorf("invalid full-text index: duplicate token %q", token)
		}
		index.tokenIDByText[token] = tokenID
		if tokenID > maxTokenID {
			maxTokenID = tokenID
		}
	}

	docSlots, err := readBoundedCount(reader, "document", 10)
	if err != nil {
		return nil, 0, err
	}
	index.docTerms = make([][]uint32, docSlots)
	index.docPKs = make([]string, docSlots)
	index.docLens = make([]uint16, docSlots)
	for docID := 0; docID < docSlots; docID++ {
		pk, err := readSizedString(reader)
		if err != nil {
			return nil, 0, fmt.Errorf("invalid full-text index document %d primary key: %w", docID, err)
		}
		var docLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &docLen); err != nil {
			return nil, 0, fmt.Errorf("invalid full-text index document %d length", docID)
		}
		termCount, err := readBoundedCount(reader, "document term", 4)
		if err != nil || termCount%2 != 0 {
			return nil, 0, fmt.Errorf("invalid full-text index document %d terms", docID)
		}
		terms := make([]uint32, termCount)
		for i := range terms {
			terms[i], err = readUint32(reader)
			if err != nil {
				return nil, 0, fmt.Errorf("invalid full-text index document %d term %d", docID, i)
			}
		}

		index.docPKs[docID] = pk
		index.docLens[docID] = docLen
		index.docTerms[docID] = terms
		if pk == "" {
			if len(terms) != 0 || docLen != 0 {
				return nil, 0, fmt.Errorf("invalid full-text index empty document %d has term data", docID)
			}
			continue
		}
		if docID == 0 {
			return nil, 0, fmt.Errorf("invalid full-text index uses reserved document id 0")
		}
		if _, duplicate := index.docByPK[pk]; duplicate {
			return nil, 0, fmt.Errorf("invalid full-text index: duplicate primary key %q", pk)
		}
		index.docByPK[pk] = uint32(docID)
		index.totalDocLen += uint64(docLen)
		for i := 0; i < len(terms); i += 2 {
			tokenID, frequency := terms[i], terms[i+1]
			if tokenID == 0 || tokenID > maxTokenID || frequency == 0 {
				return nil, 0, fmt.Errorf("invalid full-text index document %d term data", docID)
			}
			index.postings[tokenID] = append(index.postings[tokenID], uint32(docID))
		}
	}
	if reader.Len() != 0 {
		return nil, 0, fmt.Errorf("invalid full-text index: trailing data")
	}

	index.nextDocID = uint32(docSlots)
	if index.nextDocID == 0 {
		index.nextDocID = 1
	}
	index.nextTokenID = maxTokenID + 1
	if index.nextTokenID == 0 {
		index.nextTokenID = 1
	}
	index.tokensDirty = true
	index.Finalize()
	return index, checkpointLSN, nil
}

func fullTextSnapshotLayout(index *FullTextIndex) ([]fullTextTokenSnapshot, int, error) {
	tokens := make([]fullTextTokenSnapshot, 0, len(index.tokenIDByText))
	for token, tokenID := range index.tokenIDByText {
		if tokenID == 0 {
			return nil, 0, fmt.Errorf("full-text index token %q has invalid id 0", token)
		}
		tokens = append(tokens, fullTextTokenSnapshot{id: tokenID, text: token})
	}
	sort.Slice(tokens, func(i, j int) bool { return tokens[i].id < tokens[j].id })
	for i := range tokens {
		if tokens[i].id != uint32(i+1) {
			return nil, 0, fmt.Errorf("full-text index has invalid token id %d", tokens[i].id)
		}
	}

	docSlots := len(index.docPKs)
	if len(index.docTerms) > docSlots {
		docSlots = len(index.docTerms)
	}
	if len(index.docLens) > docSlots {
		docSlots = len(index.docLens)
	}
	return tokens, docSlots, nil
}

func writeFullTextIndexPayload(writer io.Writer, index *FullTextIndex, tokens []fullTextTokenSnapshot, docSlots int) error {
	if err := writeUint32(writer, uint32(len(tokens))); err != nil {
		return err
	}
	for _, token := range tokens {
		if err := writeUint32(writer, token.id); err != nil {
			return err
		}
		if err := writeSizedString(writer, token.text); err != nil {
			return err
		}
	}
	if err := writeUint32(writer, uint32(docSlots)); err != nil {
		return err
	}
	for i := 0; i < docSlots; i++ {
		pk := ""
		if i < len(index.docPKs) {
			pk = index.docPKs[i]
		}
		if err := writeSizedString(writer, pk); err != nil {
			return err
		}
		var docLen uint16
		if i < len(index.docLens) {
			docLen = index.docLens[i]
		}
		if err := writeUint16(writer, docLen); err != nil {
			return err
		}
		var terms []uint32
		if i < len(index.docTerms) {
			terms = index.docTerms[i]
		}
		if len(terms)%2 != 0 {
			return fmt.Errorf("full-text index document %d has malformed term data", i)
		}
		if err := writeUint32(writer, uint32(len(terms))); err != nil {
			return err
		}
		for _, term := range terms {
			if err := writeUint32(writer, term); err != nil {
				return err
			}
		}
	}
	return nil
}

type fullTextChecksumWriter struct {
	writer   io.Writer
	checksum uint32
}

func (writer *fullTextChecksumWriter) Write(data []byte) (int, error) {
	n, err := writer.writer.Write(data)
	if n > 0 {
		writer.checksum = util.CRC32Update(writer.checksum, data[:n])
	}
	return n, err
}

func writeUint16(writer io.Writer, value uint16) error {
	var encoded [2]byte
	binary.LittleEndian.PutUint16(encoded[:], value)
	return writeBytes(writer, encoded[:])
}

func writeUint32(writer io.Writer, value uint32) error {
	var encoded [4]byte
	binary.LittleEndian.PutUint32(encoded[:], value)
	return writeBytes(writer, encoded[:])
}

func writeSizedString(writer io.Writer, value string) error {
	if uint64(len(value)) > uint64(^uint32(0)) {
		return fmt.Errorf("full-text index string is too large")
	}
	if err := writeUint32(writer, uint32(len(value))); err != nil {
		return err
	}
	n, err := io.WriteString(writer, value)
	if err == nil && n != len(value) {
		err = io.ErrShortWrite
	}
	return err
}

func writeBytes(writer io.Writer, data []byte) error {
	n, err := writer.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

func readUint32(reader *bytes.Reader) (uint32, error) {
	var encoded [4]byte
	if _, err := reader.Read(encoded[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(encoded[:]), nil
}

func readBoundedCount(reader *bytes.Reader, label string, minimumBytes int) (int, error) {
	count, err := readUint32(reader)
	if err != nil {
		return 0, fmt.Errorf("invalid full-text index %s count", label)
	}
	if minimumBytes <= 0 || uint64(count) > uint64(reader.Len()/minimumBytes)+1 {
		return 0, fmt.Errorf("invalid full-text index %s count %d", label, count)
	}
	return int(count), nil
}

func readSizedString(reader *bytes.Reader) (string, error) {
	length, err := readUint32(reader)
	if err != nil {
		return "", err
	}
	if uint64(length) > uint64(reader.Len()) {
		return "", fmt.Errorf("truncated string")
	}
	data := make([]byte, int(length))
	if _, err := reader.Read(data); err != nil {
		return "", err
	}
	return string(data), nil
}
