package send

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/spf13/viper"
)

const (
	MaxBufferThreshold = 4 * 1024 * 1024 // 4MB
	LowBufferThreshold = 2 * 1024 * 1024 // 2MB
	MaxSafeChunkSize   = 65535 // almost 64kb, but not rly
	progressInterval   = 500 * time.Millisecond
)

var ChunkSize int

type FileMetadata struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func HandleFileSend(dc *webrtc.DataChannel, filePath string) {
	ChunkSize = viper.GetInt("chunkSize")
	if ChunkSize > MaxSafeChunkSize {
		ChunkSize = MaxSafeChunkSize
	}
	fmt.Printf("Data channel open, starting transfer of: %s\n", filePath)

	file, err := os.Open(filePath)
	if err != nil {
		log.Printf("Failed to open file: %v", err)
		return
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		log.Printf("[!] Failed to read file stats: %v", err)
		return
	}

	meta := FileMetadata{
		Name: filepath.Base(filePath),
		Size: fileInfo.Size(),
	}

	metaBytes, _ := json.Marshal(meta)
	if err := dc.SendText(string(metaBytes)); err != nil {
		log.Printf("[!] Failed to send metadata: %v", err)
		return
	}

	resumeChan := make(chan struct{}, 1)
	dc.SetBufferedAmountLowThreshold(LowBufferThreshold)
	dc.OnBufferedAmountLow(func() {
		select {
		case resumeChan <- struct{}{}:
		default:
		}
	})

	buffer := make([]byte, ChunkSize)
	startTime := time.Now()
	var totalSent int64
	lastPrint := time.Now()

	printProgress := func() {
		percentage := (float64(totalSent) / float64(meta.Size)) * 100
		fmt.Printf("\r[*] Sending: %.2f%% (%.2f / %.2f MB)",
			percentage, float64(totalSent)/(1024*1024), float64(meta.Size)/(1024*1024))
	}

	for {
		if dc.BufferedAmount() > MaxBufferThreshold {
			<-resumeChan
		}

		n, err := file.Read(buffer)
		if n > 0 {
			if err := dc.Send(buffer[:n]); err != nil {
				log.Printf("Error sending chunk: %v", err)
				return
			}
			totalSent += int64(n)

			if time.Since(lastPrint) > progressInterval {
				printProgress()
				lastPrint = time.Now()
			}
		}
		if err == io.EOF {
			printProgress()
			break
		}
	}
	duration := time.Since(startTime)
	fmt.Printf("\n[*] Finished! Sent %.2f MB in %v (Avg: %.2f MB/s)\n",
		float64(totalSent)/(1024*1024), duration, (float64(totalSent)/(1024*1024))/duration.Seconds())

	dc.SendText("EOF")
}
