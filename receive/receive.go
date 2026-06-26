package receive

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/pion/webrtc/v3"
	"github.com/schollz/progressbar/v3"
)

type FileMetadata struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

func HandleFileReceive(dc *webrtc.DataChannel, targetDir string) {
	fmt.Printf("[*] Data channel open! Ready to receive into: %s\n", targetDir)

	var file *os.File
	var bufferedWriter *bufio.Writer
	var meta FileMetadata
	var totalReceived int64
	initialized := false
	var bar *progressbar.ProgressBar

	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		if msg.IsString {
			textCmd := string(msg.Data)
			if textCmd == "EOF" {
				if bufferedWriter != nil {
					bufferedWriter.Flush()
				}
				if file != nil {
					file.Close()
				}
				if bar != nil {
					bar.Finish()
				}
				fmt.Println("\n[*] File download completed successfully!")
				os.Exit(0)
			}
			if !initialized {
				if err := json.Unmarshal(msg.Data, &meta); err != nil {
					log.Fatalf("[!] Failed to parse metadata packet: %v", err)
				}

				finalPath := filepath.Join(targetDir, meta.Name)

				var partialSize int64
				if stat, err := os.Stat(finalPath); err == nil {
					partialSize = stat.Size()
					if partialSize >= meta.Size {
						partialSize = 0
					}
				}

				dc.SendText(fmt.Sprintf("RESUME %d", partialSize))

				var err error
				if partialSize > 0 {
					file, err = os.OpenFile(finalPath, os.O_APPEND|os.O_WRONLY, 0644)
					fmt.Printf("[*] Resuming download of: %s (%.2f MB already received)\n",
						meta.Name, float64(partialSize)/(1024*1024))
				} else {
					file, err = os.Create(finalPath)
				}
				if err != nil {
					log.Fatalf("[!] Failed to create local file: %v", err)
				}

				bufferedWriter = bufio.NewWriterSize(file, 1024*1024)
				bar = progressbar.NewOptions64(meta.Size,
					progressbar.OptionSetDescription("Downloading"),
					progressbar.OptionSetWidth(40),
					progressbar.OptionShowBytes(true),
					progressbar.OptionShowCount(),
					progressbar.OptionSetPredictTime(true),
					progressbar.OptionSetRenderBlankState(true),
				)
				if partialSize > 0 {
					bar.Add(int(partialSize))
				}
				initialized = true
				return
			}
		}

		if initialized {
			n, err := bufferedWriter.Write(msg.Data)
			if err != nil {
				log.Printf("[!] Disk write error: %v", err)
				return
			}
			totalReceived += int64(n)
			bar.Add(n)
		}
	})
}
