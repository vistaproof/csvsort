package main

import (
	"container/heap"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/jfcg/sorty/v2"
)

var tmpdir string
var src string
var dst string
var mem int
var fieldsOrder string
var header bool

func init() {
	flag.StringVar(&tmpdir, "tmpdir", "/tmp", "temporary directory")
	flag.StringVar(&src, "src", "", "source file")
	flag.StringVar(&dst, "dst", "", "destination file")
	flag.StringVar(&fieldsOrder, "fields-order", "", "comma separated list of fields order eg: 0,2,3 or 1,0")
	flag.IntVar(&mem, "mem", 256, "memory limit MB")
	flag.BoolVar(&header, "header", false, "exclude header from sorting")

	flag.Parse()
}

func toSortFields(s string) []int {
	var fields []int
	for _, f := range strings.Split(s, ",") {
		i, err := strconv.Atoi(f)
		if err != nil {
			log.Fatal("invalid sort-order", err)
		}
		fields = append(fields, i)
	}
	if len(fields) == 0 {
		return []int{0}
	}
	return fields
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	out := os.Stdout
	if dst != "" {
		wf, err := os.Create(dst)
		if err != nil {
			return err
		}
		out = wf
	}

	reader := os.Stdin
	if src != "" {
		rf, err := os.Open(src)
		if err != nil {
			return err
		}
		reader = rf
	}

	sortWriter, err := New(out, mem<<20)
	if err != nil {
		return err
	}
	return sortWriter.SortFrom(reader)
}

type CSVSort struct {
	tmpDir     string
	out        *csv.Writer
	memLimit   int
	memUsed    int
	numFiles   int
	sortFields []int
	header     []string
	vals       [][]string
}

func New(out io.Writer, memLimit int) (*CSVSort, error) {
	tmpDir := filepath.Join(tmpdir, "csvsort-tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return nil, err
	}
	return &CSVSort{
		tmpDir:     tmpDir,
		out:        csv.NewWriter(out),
		sortFields: toSortFields(fieldsOrder),
		memLimit:   memLimit,
	}, nil
}

func (s *CSVSort) SortFrom(reader io.Reader) error {
	csvReader := csv.NewReader(reader)

	for i := 0; ; i++ {
		record, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if i == 0 && header {
			s.header = append(s.header, record...)
			continue
		}
		if err := s.writeRecord(record); err != nil {
			return err
		}
	}

	return s.Close()
}

func (s *CSVSort) writeRecord(b []string) error {
	s.vals = append(s.vals, b)

	var memUsed int
	for i := range b {
		memUsed += len(b[i])
	}
	s.memUsed += memUsed
	if s.memUsed >= s.memLimit {
		flushErr := s.flush()
		if flushErr != nil {
			return flushErr
		}
	}
	return nil
}

func (s *CSVSort) flush() error {
	inmem := &inmemory{s.vals, s.sortFields}
	sorty.Sort(inmem.Len(), inmem.Lesswap)
	file, err := os.Create(filepath.Join(s.tmpDir, strconv.Itoa(s.numFiles)))
	if err != nil {
		return err
	}
	s.numFiles++
	s.memUsed = 0
	out := csv.NewWriter(file)
	for _, val := range s.vals {
		if err := out.Write(val); err != nil {
			file.Close()
			return err
		}
	}
	out.Flush()
	if closeErr := file.Close(); closeErr != nil {
		return closeErr
	}
	s.vals = s.vals[:0]
	return out.Error()
}

func (s *CSVSort) Close() error {
	defer os.RemoveAll(s.tmpDir)

	if s.memUsed > 0 {
		flushErr := s.flush()
		if flushErr != nil {
			return flushErr
		}
	}

	// Free memory used by last read vals
	s.vals = nil

	files := make(map[int]*csv.Reader, s.numFiles)

	closers := make([]io.Closer, 0, s.numFiles)
	for i := 0; i < s.numFiles; i++ {
		file, err := os.Open(filepath.Join(s.tmpDir, strconv.Itoa(i)))
		if err != nil {
			return fmt.Errorf("unable to open temp file: %v", err)
		}
		closers = append(closers, file)
		files[i] = csv.NewReader(file)
	}
	defer func() {
		for i := range closers {
			closers[i].Close()
		}
	}()

	// Need to perform final sort across intermediary files
	if err := s.finalSort(files); err != nil {
		return err
	}
	s.out.Flush()
	return s.out.Error()
}

func (s *CSVSort) finalSort(files map[int]*csv.Reader) error {
	entries := &entryHeap{sortFields: s.sortFields}
	perFileLimit := s.memLimit / (s.numFiles + 1)
	fillBuffer := func() error {
		for i := 0; i < len(files); i++ {
			csvField := files[i]
			amountRead := 0
			for {
				records, err := csvField.Read()
				if err != nil {
					if err == io.EOF {
						delete(files, i)
						break
					}
					return fmt.Errorf("Error filling buffer: %v", err)
				}

				for j := 0; j < len(records); j++ {
					amountRead += len(records[j])
				}
				heap.Push(entries, &entry{i, records})
				if amountRead >= perFileLimit {
					break
				}
			}
		}
		return nil
	}

	for i := 0; ; i++ {
		if len(entries.entries) == 0 {
			if err := fillBuffer(); err != nil {
				return err
			}
			if len(entries.entries) == 0 {
				// Nothing left with which to fill buffer, stop
				break
			}
		}

		if i == 0 && len(s.header) > 0 {
			if err := s.out.Write(s.header); err != nil {
				return fmt.Errorf("error writing to final output: %v", err)
			}
			continue
		}

		e := heap.Pop(entries).(*entry)
		if err := s.out.Write(e.val); err != nil {
			return fmt.Errorf("error writing to final output: %v", err)
		}
		file := files[e.fileIdx]
		if file != nil {
			record, err := file.Read()
			if err != nil {
				if err == io.EOF {
					delete(files, e.fileIdx)
					continue
				}
				log.Println(record)
				return fmt.Errorf("error replacing entry on heap: %v, %s", err, filepath.Join(s.tmpDir, strconv.Itoa(e.fileIdx)))
			}

			heap.Push(entries, &entry{e.fileIdx, record})
		}
	}
	return nil
}

type inmemory struct {
	vals       [][]string
	sortFields []int
}

func (im *inmemory) Len() int {
	return len(im.vals)
}

// Lesswap function operates on an underlying collection to be sorted as:
//  if less(i, k) { // strict ordering like < or >
//  	if r != s {
//  		swap(r, s)
//  	}
//  	return true
//  }
//  return false
func (im *inmemory) Lesswap(i, k, r, s int) bool {
	if im.Less(i, k) {
		if r != s {
			im.Swap(r, s)
		}
		return true
	}
	return false
}

func (im *inmemory) Less(i, j int) bool {
	for _, field := range im.sortFields {
		if im.vals[i][field] < im.vals[j][field] {
			return true
		} else if im.vals[i][field] > im.vals[j][field] {
			return false
		}
	}
	return false
}

func (im *inmemory) Swap(i, j int) {
	im.vals[i], im.vals[j] = im.vals[j], im.vals[i]
}

type entry struct {
	fileIdx int
	val     []string
}

type entryHeap struct {
	entries    []*entry
	sortFields []int
}

func (eh *entryHeap) Len() int {
	return len(eh.entries)
}

func (eh *entryHeap) Less(i, j int) bool {
	for _, field := range eh.sortFields {
		if eh.entries[i].val[field] < eh.entries[j].val[field] {
			return true
		} else if eh.entries[i].val[field] > eh.entries[j].val[field] {
			return false
		}
	}
	return false
}

func (eh *entryHeap) Swap(i, j int) {
	eh.entries[i], eh.entries[j] = eh.entries[j], eh.entries[i]
}

func (eh *entryHeap) Push(x interface{}) {
	eh.entries = append(eh.entries, x.(*entry))
}

func (eh *entryHeap) Pop() interface{} {
	n := len(eh.entries)
	x := eh.entries[n-1]
	eh.entries = eh.entries[:n-1]
	return x
}
