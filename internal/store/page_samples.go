package store

import (
	"bufio"
	"encoding/json"
	"io"
	"io/fs"
	"os"

	"gwpool/internal/types"
)

// PageSampleCursor is an opaque position in the append-only sample file.
// It intentionally retains no sample data or open file descriptor.
type PageSampleCursor struct {
	file   fs.FileInfo
	offset int64 // after the last newline, not after a provisional EOF record
}

type PageSampleDelta struct {
	Rows   []types.Sample
	Cursor PageSampleCursor
	Reset  bool
	Tail   bool // last returned row has no newline and must be re-read next time
}

// ReadPageSamples preserves Samples' JSON/type validation and EOF semantics,
// while returning only the newly appended page projection. Callers must discard
// the entire delta on error and never mutate previously published sample rows.
func (s *Store) ReadPageSamples(previous PageSampleCursor) (PageSampleDelta, error) {
	file, info, err := s.sampleFileSnapshot()
	if err != nil {
		return PageSampleDelta{}, err
	}
	if file == nil {
		return PageSampleDelta{Reset: true}, nil
	}
	defer func() { _ = file.Close() }()
	reset := previous.file == nil || !os.SameFile(previous.file, info) ||
		info.Size() < previous.file.Size() ||
		(info.Size() == previous.file.Size() && !info.ModTime().Equal(previous.file.ModTime()))
	offset := previous.offset
	if reset {
		offset = 0
	}
	result := PageSampleDelta{Reset: reset, Cursor: PageSampleCursor{file: info, offset: offset}}
	scanner := bufio.NewScanner(io.NewSectionReader(file, offset, info.Size()-offset))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	complete := false
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		advance, token, err := bufio.ScanLines(data, atEOF)
		if advance > 0 {
			complete = data[advance-1] == '\n'
			if complete {
				result.Cursor.offset += int64(advance)
			}
		}
		return advance, token, err
	})
	for scanner.Scan() {
		var sample types.Sample
		if json.Unmarshal(scanner.Bytes(), &sample) != nil {
			continue
		}
		// Decode the original type first: ignoring unused fields during JSON
		// decoding would accept malformed historical rows Samples rejected.
		result.Rows = append(result.Rows, pageSample(sample))
		result.Tail = !complete
	}
	if err := scanner.Err(); err != nil {
		return PageSampleDelta{}, err
	}
	return result, nil
}

func pageSample(sample types.Sample) types.Sample {
	sample.Observed.IP, sample.Observed.ISP = "", ""
	sample.InventoryPrediction = nil
	if sample.Err != "" {
		sample.Err = "error"
	}
	if sample.Route != nil {
		sample.Route = &types.RouteSample{Result: sample.Route.Result}
	}
	return sample
}
