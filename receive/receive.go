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

				var err error
				file, err = os.Create(finalPath)
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
