package duckdb

import (
	"archive/zip"
	"bytes"
	"fmt"
	"github.com/cespare/xxhash/v2"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"runtime"
	"time"
)

func InstallX() error {
	var dist = runtime.GOOS + "/" + runtime.GOARCH
	return Download(dist, "", nil)
}

func Install(tag string) error {

	var m = map[string][]byte{}

	if err := Download("darwin/arm64", tag, m); err != nil {
		return err
	} else if err := Download("linux/amd64", tag, m); err != nil {
		return err
	}

	var dir = "/var/tmp/duckdb-build/"

	if tag == "latest" {
		var h uint64
		for _, v := range m {
			h ^= xxhash.Sum64(v)
		}
		dir += fmt.Sprintf("latest_%s_%d", time.Now().Format(time.DateOnly), h&0xffffffff)
	} else {
		dir += tag
	}

	log.Println("downloaded duckdb files to ", dir)

	_, err := os.Stat(dir)
	if err == nil {
		log.Println("dir exist", dir)
		return nil
	}

	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		return err
	}

	for k, v := range m {
		if err := os.WriteFile(path.Join(dir, k), v, 0666); err != nil {
			return err
		}
	}

	return nil
}

func Download(dist string, tag string, m map[string][]byte) error {
	switch dist {
	case "darwin/arm64", "darwin/amd64":
		var dl, subDir string
		switch tag {
		case "v1.0.0":
			dl = "https://github.com/duckdb/duckdb/releases/download/" + tag + "/libduckdb-osx-universal.zip"
		case "", "latest":
			dl = "https://artifacts.duckdb.org/latest/duckdb-binaries-osx.zip"
			subDir = "libduckdb-osx-universal.zip"
		default:
			return fmt.Errorf("invalid tag %s", tag)
		}

		return download(dl, subDir, m)
	case "linux/amd64":
		var dl, subDir string
		switch tag {
		case "v1.0.0":
			dl = "https://github.com/duckdb/duckdb/releases/download/" + tag + "/libduckdb-linux-amd64.zip"
		case "", "latest":
			dl = "https://artifacts.duckdb.org/latest/duckdb-binaries-linux.zip"
			subDir = "libduckdb-linux-amd64.zip"
		}

		return download(dl, subDir, m)
	}
	return fmt.Errorf("invalid dist")
}

func download(dl, subDir string, m map[string][]byte) error {
	resp, err := http.Get(dl)
	if err != nil {
		return err
	}
	if resp.StatusCode != 200 {
		return fmt.Errorf("invalid status %s", resp.Status)
	}

	buf, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	rd, err := zip.NewReader(bytes.NewReader(buf), int64(len(buf)))
	if err != nil {
		return err
	}

	if subDir != "" {
		f, err := rd.Open(subDir)
		if err != nil {
			for _, v := range rd.File {
				log.Println(v.Name)
			}
			return err
		}

		sz, err := f.Stat()
		if err != nil {
			return err
		}

		buf = buf[:sz.Size()]
		if _, err := io.ReadFull(f, buf); err != nil {
			return err
		}

		rd, err = zip.NewReader(bytes.NewReader(buf), sz.Size())
		if err != nil {
			return err
		}
	}

	for _, f := range rd.File {
		b, err := copyFile(f)
		if err != nil {
			return err
		}
		if old := m[f.Name]; len(old) > 0 && !bytes.Equal(old, b) {
			return fmt.Errorf("ambigous content for file %s", f.Name)
		}
		m[f.Name] = b
	}
	return nil

}

func copyFile(f *zip.File) ([]byte, error) {
	ff, err := f.Open()
	if err != nil {
		return nil, err
	}
	defer ff.Close()
	return io.ReadAll(ff)
}
