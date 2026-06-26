# Burrow

Peer-to-peer encrypted file sharing over WebRTC.

## Install

```sh
git clone https://github.com/KillAllChickens/burrow
cd burrow
go install .
```

## Usage

Sender - share a file:

```sh
burrow start -f myfile.txt
```

This prints a session code:

```
[*] Initializing file sharing server...
[*] Session Created! Code: 452996
[*] Waiting for peer to join...
```

Once a receiver joins with the code, the file transfers immediately.

Receiver - download a file:

```sh
burrow join 452996 -o ~/Downloads
```

The file is saved to the output directory with its original filename.

## Flags

| Flag | Description | Default |
|------|-------------|---------|
| `-f, --file` | File to share (start) | |
| `-o, --output` | Output directory (join) | `.` |

## Config

Settings are loaded from `~/.config/burrow/config.yaml` or `BURR_*` env vars. Default config:

```yaml
chunkSize: 65536
server: api.killallchickens.org
stun: stun:stun.l.google.com:19302
```
The default signaling server is `api.killallchickens.org`, a public instance that is always online. You can also self-host your own signaling server and point to it with the config.

## How it works

1. Sender creates a session on the signaling server and gets a code.
2. Receiver joins with the code.
3. Both peers exchange SDP/ICE over WebSocket.
4. A direct WebRTC data channel opens between peers.
5. File is streamed in encrypted 64 KB chunks with flow control.

The file transfer is encrypted end-to-end via WebRTC's mandatory DTLS.
