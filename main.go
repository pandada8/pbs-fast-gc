package main

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"io/fs"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vbauerster/mpb"
	"github.com/vbauerster/mpb/decor"
	"golang.org/x/sync/errgroup"
)

var (
	baseDir        = flag.String("baseDir", "", "Base directory to start scanning from")
	metadataWorker = flag.Int("metadataWorker", 10, "Number of workers to use for metadata scanning")
	blobsWorker    = flag.Int("blobsWorker", 10, "Number of workers to use for blobs scanning")
	doDelete       = flag.Bool("delete", false, "actual delete the file")
	deleteWorker   = flag.Int("deleteWorker", 10, "Number of workers to use for blobs deleting")
)

type fileid [32]byte

func div(a, b int) int {
	return int(math.Ceil(float64(a) / float64(b)))
}

func indexCollector(p *mpb.Progress) (AllIndex map[fileid]struct{}) {
	// 1. collecting all fidx and didx
	bar := p.AddBar(3, mpb.BarPriority(1), mpb.PrependDecorators(
		// simple name decorator
		decor.Name("1. metadata files"),
		// decor.DSyncWidth bit enables column width synchronization,
		decor.Percentage(decor.WCSyncSpace),
		decor.CountersNoUnit("%d / %d", decor.WCSyncSpace),
	))
	indexes := []string{}
	for _, subdir := range []string{"vm", "ns", "ct"} {
		dir := filepath.Join(*baseDir, subdir)
		if _, err := os.Stat(dir); err != nil {
			bar.Increment()
			continue
		}
		filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if filepath.Ext(path) == ".fidx" || filepath.Ext(path) == ".didx" {
				indexes = append(indexes, path)
			}
			return nil
		})
		bar.Increment()
	}

	// shuffle the index
	rand.Shuffle(len(indexes), func(i, j int) { indexes[i], indexes[j] = indexes[j], indexes[i] })

	bar = p.AddBar(int64(len(indexes)), mpb.BarPriority(2), mpb.PrependDecorators(
		// simple name decorator
		decor.Name("2. metadata"),
		// decor.DSyncWidth bit enables column width synchronization,
		decor.Percentage(decor.WCSyncSpace),
		decor.CountersNoUnit("%d / %d", decor.WCSyncSpace),
	))
	eg, _ := errgroup.WithContext(context.Background())
	AllIndex = make(map[fileid]struct{})
	var allIndexLock sync.Mutex

	chunkSize := div(len(indexes), *metadataWorker)
	for i := 0; i < *metadataWorker; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == *metadataWorker-1 {
			end = len(indexes)
		}
		eg.Go(func() error {
			localIndexes := make(map[fileid]struct{})
			for _, file := range indexes[start:end] {
				// read file and add to local index
				payload, err := os.ReadFile(file)
				if err != nil {
					panic(err)
				}
				// skip
				if strings.HasSuffix(file, "fidx") {
					for offset := 4096; offset < len(payload); offset += 32 {
						localIndexes[fileid(payload[offset:offset+32])] = struct{}{}
					}
				} else if strings.HasSuffix(file, "didx") {
					// https://pbs.proxmox.com/docs/file-formats.html
					for offset := 4096; offset < len(payload); offset += 40 {
						localIndexes[fileid(payload[offset+8:offset+40])] = struct{}{}
					}
				}
				bar.Increment()
			}
			allIndexLock.Lock()
			for k, v := range localIndexes {
				AllIndex[k] = v
			}
			allIndexLock.Unlock()
			return nil
		})
	}
	eg.Wait()
	return
}

func blobsCollector(bar *mpb.Bar) (AllBlob []fileid) {
	dir := filepath.Join(*baseDir, ".chunks")
	// 0x0 - 0xffff
	chunkSize := div(65536, *blobsWorker)
	eg, _ := errgroup.WithContext(context.Background())
	var allBlobLock sync.Mutex
	for i := 0; i < *blobsWorker; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if i == *blobsWorker-1 {
			end = 65536
		}
		eg.Go(func() error {
			localBlobs := []fileid{}
			for j := start; j < end; j++ {
				entries, err := os.ReadDir(filepath.Join(dir, fmt.Sprintf("%04x", j)))
				if err != nil {
					panic(err)
				}
				for _, entry := range entries {
					fid, err := hex.DecodeString(entry.Name())
					if err != nil {
						continue
					}
					localBlobs = append(localBlobs, fileid(fid))
				}
				bar.Increment()
			}
			allBlobLock.Lock()
			AllBlob = append(AllBlob, localBlobs...)
			allBlobLock.Unlock()
			return nil
		})
	}
	eg.Wait()
	return
}

func Delete(pending []string) {
	start := time.Now()
	p := mpb.New()
	bar := p.AddBar(int64(len(pending)), mpb.PrependDecorators(
		// display our name with one space on the right
		decor.Name("delete"),
		decor.CountersNoUnit("%d / %d", decor.WCSyncSpace),
	),

		mpb.AppendDecorators(decor.Percentage()),
	)
	// TODO: parallel delete
	eg, _ := errgroup.WithContext(context.Background())
	chunkSize := div(len(pending), *deleteWorker)

	for i := 0; i < *deleteWorker; i++ {
		start := i * chunkSize
		end := i*chunkSize + chunkSize
		if i == *deleteWorker-1 {
			end = len(pending)
		}
		eg.Go(func() error {
			for _, f := range pending[start:end] {
				os.Remove(f)
				bar.Increment()
			}
			return nil
		})
	}

	eg.Wait()
	p.Wait()
	fmt.Printf("all files deleted, using %s\n", time.Since(start))
}

func main() {
	start := time.Now()
	flag.Parse()
	if *baseDir == "" {
		fmt.Println("baseDir is required")
		flag.Usage()
		return
	}
	p := mpb.New()

	var (
		Index map[fileid]struct{}
		Blobs []fileid
	)
	eg, _ := errgroup.WithContext(context.Background())
	eg.Go(func() error {

		Index = indexCollector(p)
		return nil
	})
	eg.Go(func() error {
		bar := p.AddBar(65536, mpb.BarPriority(3), mpb.PrependDecorators(
			// simple name decorator
			decor.Name("3. blobs"),
			// decor.DSyncWidth bit enables column width synchronization,
			decor.Percentage(decor.WCSyncSpace),
			decor.CountersNoUnit("%d / %d", decor.WCSyncSpace),
		))
		Blobs = blobsCollector(bar)
		return nil
	})

	err := eg.Wait()
	if err != nil {
		panic(err)
	}
	p.Wait()
	fmt.Printf("all collecting finished, index %d, blobs: %d\n", len(Index), len(Blobs))
	ShouldDelete := []string{}
	// TODO: parallel diff
	for _, f := range Blobs {
		if _, ok := Index[f]; !ok {
			encoded := hex.EncodeToString(f[:])
			ShouldDelete = append(ShouldDelete, filepath.Join(*baseDir, ".chunks", encoded[:4], encoded))
		}
	}
	sort.Strings(ShouldDelete)
	fmt.Printf("%d files should be deleted\n", len(ShouldDelete))
	name := "delete-" + time.Now().Format(time.DateTime) + ".txt"
	os.WriteFile(name, []byte(strings.Join(ShouldDelete, "\n")), 0644)
	fmt.Printf("file list write at %s\n", name)

	fmt.Println("scan finished using", time.Since(start))

	if *doDelete {
		Delete(ShouldDelete)
	}
}
