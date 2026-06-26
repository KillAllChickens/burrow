# Burrow

Peer-to-peer encrypted file sharing over WebRTC.

## Install

```sh
go install
```

## Usage

**Sender:** share a file and get a session code.

```sh
burrow start -f myfile.txt
```
```
[*] Initializing file sharing server...
[*] Session Created! Code: <CODE>
[*] Waiting for peer to join...
```

**Receiver:** use that session code from above to download the file.

```sh
burrow join <code> -o ~/Downloads
```

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-p, --port` | `9182` | Signaling server port |
| `-s, --server` | `api.killallchickens.org` | Signaling server |
| `-f, --file` | | File to share (start) |
| `-o, --output` | `.` | Output directory (join) |

## Config

Settings can be set in `~/.config/burrow/config.yaml` or via `BURR_*` env vars (e.g. `BURR_SERVER`, `BURR_PORT`, `BURR_STUN`, `BURR_CHUNKSIZE`).  

A public server will always be available if you set your server to `api.killallchickens.org`

## How it works

1. Sender creates a session on the signaling server and gets a code.
2. Receiver joins with the code.
3. Both peers exchange SDP/ICE over WebSocket.
4. A direct WebRTC data channel opens between peers.
5. File is streamed in encrypted 64 KB chunks with flow control.

The file transfer is encrypted end-to-end via WebRTC's mandatory DTLS/SRTP.
