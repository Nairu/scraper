package ds

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/sselph/scraper/ovgdb-dl"
)

const (
	zipURL   = "https://storage.googleapis.com/stevenselph.appspot.com/openvgdb2.zip"
	dbName   = "ldb"
	zipName  = "openvgdb.zip"
	metaName = "openvgdb.meta"
)

func ovgdbUnmarshalGame(b []byte) (*Game, error) {
	var s []string
	err := json.Unmarshal(b, &s)
	if err != nil {
		return nil, err
	}
	if len(s) != 9 {
		return nil, fmt.Errorf("length of slice must be 9 but was %d", len(s))
	}
	g := NewGame()
	g.ID = s[0]
	g.GameTitle = s[1]
	g.Overview = s[2]
	g.Developer = s[3]
	g.Publisher = s[4]
	g.Genre = s[5]
	g.ReleaseDate = s[6]
	g.Source = s[7]
	if s[8] != "" {
		g.Images[ImgBoxart] = HTTPImage{URL: s[8]}
		g.Thumbs[ImgBoxart] = HTTPImage{URL: s[8]}
	}
	return g, nil
}

// OVGDB is a DataSource using OpenVGDB.
type OVGDB struct {
	db     *leveldb.DB
	Hasher *Hasher
}

// GetName implements DS.
func (o *OVGDB) GetName(p string) string {
	h, err := o.Hasher.Hash(p)
	if err != nil {
		return ""
	}
	n, err := o.db.Get([]byte(h+"-name"), nil)
	if err != nil {
		return ""
	}
	return string(n)
}

// getID gets the ID from the path.
func (o *OVGDB) getID(p string) (string, error) {
	h, err := o.Hasher.Hash(p)
	if err != nil {
		return "", err
	}
	id, err := o.db.Get([]byte(h), nil)
	if err == nil {
		return string(id), nil
	}
	if err != nil && err != leveldb.ErrNotFound {
		return "", err
	}
	b := filepath.Base(p)
	n := b[:len(b)-len(filepath.Ext(b))]
	id, err = o.db.Get([]byte(strings.ToLower(n)), nil)
	if err != nil {
		return "", ErrNotFound
	}
	return string(id), nil
}

// GetGame implements DS.
func (o *OVGDB) GetGame(ctx context.Context, p string) (*Game, error) {
	id, err := o.getID(p)
	if err != nil {
		return nil, err
	}
	g, err := o.db.Get([]byte(id), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return ovgdbUnmarshalGame(g)
}

// Close closes the DB.
func (o *OVGDB) Close() error {
	return o.db.Close()
}

func getDB(ctx context.Context, p string, u bool) (*leveldb.DB, error) {
	var err error
	if p == "" {
		p, err = DefaultCachePath()
		if err != nil {
			return nil, err
		}
	}
	err = mkDir(p)
	if err != nil {
		return nil, err
	}
	fp := filepath.Join(p, dbName)
	if !exists(fp) || u {
		err = ovgdbdl.RefreshCache(p)
		if err != nil {
			return nil, err
		}
	}
	db, err := leveldb.OpenFile(fp, nil)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// NewOVGDB returns a new OVGDB. OVGDB should be closed when not needed.
func NewOVGDB(ctx context.Context, h *Hasher, u bool) (*OVGDB, error) {
	db, err := getDB(ctx, "", u)
	if err != nil {
		return nil, err
	}
	return &OVGDB{Hasher: h, db: db}, nil
}
