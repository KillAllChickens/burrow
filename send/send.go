package send

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pion/webrtc/v3"
	"github.com/schollz/progressbar/v3"
	"github.com/spf13/viper"
)

const (
	MaxBufferThreshold = 4 * 1024 * 1024
	LowBufferThreshold = 2 * 1024 * 1024
	MaxSafeChunkSize   = 65535
	resumeTimeout      = 5 * time.Second
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

	resumePos := make(chan int64, 1)
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			text := string(msg.Data)
			if strings.HasPrefix(text, "RESUME ") {
				pos, _ := strconv.ParseInt(text[7:], 10, 64)
				resumePos <- pos
			}
		}
	})

	var pos int64
	select {
	case pos = <-resumePos:
	case <-time.After(resumeTimeout):
	}

	if pos > 0 {
		if _, err := file.Seek(pos, io.SeekStart); err != nil {
			log.Printf("[!] Failed to seek to resume position %d: %v", pos, err)
			pos = 0
		}
	}

	bar := progressbar.NewOptions64(meta.Size,
		progressbar.OptionSetDescription("Sending"),
		progressbar.OptionSetWidth(40),
		progressbar.OptionShowBytes(true),
		progressbar.OptionShowCount(),
		progressbar.OptionSetPredictTime(true),
		progressbar.OptionSetRenderBlankState(true),
	)
	if pos > 0 {
		bar.Add(int(pos))
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
			bar.Add(n)
		}
		if err == io.EOF {
			break
		}
	}
	bar.Finish()
	duration := time.Since(startTime)
	fmt.Printf("\n[*] Finished! Sent %.2f MB in %v (Avg: %.2f MB/s)\n",
		float64(totalSent)/(1024*1024), duration, (float64(totalSent)/(1024*1024))/duration.Seconds())

	dc.SendText("EOF")
	os.Exit(0)
}
