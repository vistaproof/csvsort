// Copyright 2011 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package csv

import (
	"bytes"
	"errors"
	"testing"
)

var writeTests = []struct {
	Input   [][][]byte
	Output  string
	Error   error
	UseCRLF bool
	Comma   rune
}{
	{Input: [][][]byte{{[]byte("abc")}}, Output: "abc\n"},
	{Input: [][][]byte{{[]byte("abc")}}, Output: "abc\r\n", UseCRLF: true},
	{Input: [][][]byte{{[]byte(`"abc"`)}}, Output: `"""abc"""` + "\n"},
	{Input: [][][]byte{{[]byte(`a"b`)}}, Output: `"a""b"` + "\n"},
	{Input: [][][]byte{{[]byte(`"a"b"`)}}, Output: `"""a""b"""` + "\n"},
	{Input: [][][]byte{{[]byte(" abc")}}, Output: `" abc"` + "\n"},
	{Input: [][][]byte{{[]byte("abc,def")}}, Output: `"abc,def"` + "\n"},
	{Input: [][][]byte{{[]byte("abc"), []byte("def")}}, Output: "abc,def\n"},
	{Input: [][][]byte{{[]byte("abc"), []byte("def")}}, Output: "abc\ndef\n"},
	{Input: [][][]byte{{[]byte("abc\ndef")}}, Output: "\"abc\ndef\"\n"},
	{Input: [][][]byte{{[]byte("abc\ndef")}}, Output: "\"abc\r\ndef\"\r\n", UseCRLF: true},
	{Input: [][][]byte{{[]byte("abc\rdef")}}, Output: "\"abcdef\"\r\n", UseCRLF: true},
	{Input: [][][]byte{{[]byte("abc\rdef")}}, Output: "\"abc\rdef\"\n", UseCRLF: false},
	{Input: [][][]byte{{[]byte("")}}, Output: "\n"},
	{Input: [][][]byte{{[]byte(""), []byte("")}}, Output: ",\n"},
	{Input: [][][]byte{{[]byte(""), []byte(""), []byte("")}}, Output: ",,\n"},
	{Input: [][][]byte{{[]byte(""), []byte(""), []byte("a")}}, Output: ",,a\n"},
	{Input: [][][]byte{{[]byte(""), []byte("a"), []byte("")}}, Output: ",a,\n"},
	{Input: [][][]byte{{[]byte(""), []byte("a"), []byte("a")}}, Output: ",a,a\n"},
	{Input: [][][]byte{{[]byte("a"), []byte(""), []byte("")}}, Output: "a,,\n"},
	{Input: [][][]byte{{[]byte("a"), []byte(""), []byte("a")}}, Output: "a,,a\n"},
	{Input: [][][]byte{{[]byte("a"), []byte("a"), []byte("")}}, Output: "a,a,\n"},
	{Input: [][][]byte{{[]byte("a"), []byte("a"), []byte("a")}}, Output: "a,a,a\n"},
	{Input: [][][]byte{{[]byte(`\.`)}}, Output: "\"\\.\"\n"},
	{Input: [][][]byte{{[]byte("x09\x41\xb4\x1c"), []byte("aktau")}}, Output: "x09\x41\xb4\x1c,aktau\n"},
	{Input: [][][]byte{{[]byte(",x09\x41\xb4\x1c"), []byte("aktau")}}, Output: "\",x09\x41\xb4\x1c\",aktau\n"},
	{Input: [][][]byte{{[]byte("a"), []byte("a"), []byte("")}}, Output: "a|a|\n", Comma: '|'},
	{Input: [][][]byte{{[]byte(","), []byte(","), []byte("")}}, Output: ",|,|\n", Comma: '|'},
	{Input: [][][]byte{{[]byte("foo")}}, Comma: '"', Error: errInvalidDelim},
}

func TestWrite(t *testing.T) {
	for n, tt := range writeTests {
		b := &bytes.Buffer{}
		f := NewWriter(b)
		f.UseCRLF = tt.UseCRLF
		if tt.Comma != 0 {
			f.Comma = tt.Comma
		}
		err := f.WriteAll(tt.Input)
		if err != tt.Error {
			t.Errorf("Unexpected error:\ngot  %v\nwant %v", err, tt.Error)
		}
		out := b.String()
		if out != tt.Output {
			t.Errorf("#%d: out=%q want %q", n, out, tt.Output)
		}
	}
}

type errorWriter struct{}

func (e errorWriter) Write(b []byte) (int, error) {
	return 0, errors.New("Test")
}

func TestError(t *testing.T) {
	b := &bytes.Buffer{}
	f := NewWriter(b)
	f.Write([][]byte{[]byte("abc")})
	f.Flush()
	err := f.Error()

	if err != nil {
		t.Errorf("Unexpected error: %s\n", err)
	}

	f = NewWriter(errorWriter{})
	f.Write([][]byte{[]byte("abc")})
	f.Flush()
	err = f.Error()

	if err == nil {
		t.Error("Error should not be nil")
	}
}

var benchmarkWriteData = [][][]byte{
	{[]byte("abc"), []byte("def"), []byte("12356"), []byte("1234567890987654311234432141542132")},
	{[]byte("abc"), []byte("def"), []byte("12356"), []byte("1234567890987654311234432141542132")},
	{[]byte("abc"), []byte("def"), []byte("12356"), []byte("1234567890987654311234432141542132")},
}

func BenchmarkWrite(b *testing.B) {
	for i := 0; i < b.N; i++ {
		w := NewWriter(&bytes.Buffer{})
		err := w.WriteAll(benchmarkWriteData)
		if err != nil {
			b.Fatal(err)
		}
		w.Flush()
	}
}
