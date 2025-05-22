package chunks

// this has to be here to avoid import cycles :/

import (
	"fmt"
	"os"
)

const CHUNK_SIZE int64 = 102400 // 100 KiB

type Chunk struct {
	Filename string
	Start    int64
	End      int64
}

func ChunkFiles(files []string) []Chunk {
	chunks := make([]Chunk, 0)
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			fmt.Printf("Invalid file path: %s\n", path)
			continue
		}

		numChunks := info.Size() / CHUNK_SIZE
		if info.Size()%CHUNK_SIZE != 0 {
			numChunks += 1
		}
		fChunks := make([]Chunk, numChunks)
		for i := range fChunks {
			end := CHUNK_SIZE*int64(i) + CHUNK_SIZE
			if end > info.Size() {
				end = info.Size()
			}
			fChunks[i] = Chunk{
				Filename: path,
				Start:    CHUNK_SIZE * int64(i),
				End:      end,
			}
		}

		chunks = append(chunks, fChunks...)
	}
	return chunks
}
