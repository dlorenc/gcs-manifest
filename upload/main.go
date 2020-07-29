package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
)

var (
	src          = flag.String("src", ".", "path to local directory or file to upload")
	dst          = flag.String("dst", "", "path to upload to on GCS")
	manifestPath = flag.String("manifest", ".", "local path to write manifest to")
)

type uploaded struct {
	sha  string
	path string
}

func main() {
	flag.Parse()
	bucketName, gcsPath, err := parseUri(*dst)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create new GCS client: %v", err)
	}

	absRoot, err := filepath.Abs(*src)
	if err != nil {
		log.Fatal(err)
	}

	bucket := client.Bucket(bucketName)
	mfst := map[string]string{}
	wg := sync.WaitGroup{}

	shaCh := make(chan uploaded)

	if err := filepath.Walk(absRoot, func(path string, fi os.FileInfo, err error) error {
		fmt.Fprintln(os.Stderr, "Uploading:", path)
		if !fi.Mode().IsRegular() {
			return nil
		}
		// We might start with a file, not a directory.
		var relPath string
		if absRoot == path {
			relPath = path
		} else {
			relPath, err = filepath.Rel(absRoot, path)
			if err != nil {
				return err
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			sha, err := uploadFile(ctx, relPath, gcsPath, bucket)
			if err != nil {
				log.Fatal(err)
			}
			shaCh <- uploaded{
				sha:  sha,
				path: relPath,
			}
			fmt.Fprintln(os.Stderr, "Uploaded:", path)
		}()
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	// Close the channel when everything is written.
	go func() {
		wg.Wait()
		close(shaCh)
	}()
	for f := range shaCh {
		mfst[f.path] = f.sha
	}

	m, err := json.Marshal(mfst)
	if err != nil {
		log.Fatal(err)
	}
	mfstObj := bucket.Object(filepath.Join(gcsPath, "manifest.json")).NewWriter(ctx)
	defer mfstObj.Close()
	if _, err := mfstObj.Write(m); err != nil {
		log.Fatal(err)
	}

	if err := ioutil.WriteFile(filepath.Join(*manifestPath, "manifest.json"), m, 0644); err != nil {
		log.Fatal(err)
	}
	fmt.Print(string(m))
}

func uploadFile(ctx context.Context, relPath string, gcsPath string, bucket *storage.BucketHandle) (string, error) {
	gcsObj := bucket.Object(filepath.Join(gcsPath, relPath)).NewWriter(ctx)
	defer gcsObj.Close()

	fmt.Fprintln(os.Stderr, "reading:", relPath)
	f, err := os.Open(relPath)
	if err != nil {
		return "", err
	}
	defer f.Close()

	// Get the hash
	h := sha256.New()
	// Setup a tee to write to GCS and the hash at the same time.
	tee := io.TeeReader(f, gcsObj)

	if _, err := io.Copy(h, tee); err != nil {
		return "", err
	}

	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

func parseUri(uri string) (string, string, error) {
	if strings.HasPrefix(uri, "gs://") {
		uri = strings.TrimPrefix(uri, "gs://")
	}
	split := strings.SplitN(uri, "/", 2)
	if len(split) != 2 {
		return "", "", fmt.Errorf("invalid uri: %s", uri)
	}
	return split[0], split[1], nil
}
