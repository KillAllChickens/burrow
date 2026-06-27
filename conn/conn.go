package conn

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/logging"
	"github.com/pion/webrtc/v3"
	"github.com/spf13/viper"
	"resty.dev/v3"
)

var httpClient *resty.Client = resty.New().SetHeader("User-Agent", "Burrow 0.1")

type PeerSession struct {
	pc      *webrtc.PeerConnection
	ws      *websocket.Conn
	wsMutex sync.Mutex
}

func (s *PeerSession) writeWS(v interface{}) error {
	s.wsMutex.Lock()
	defer s.wsMutex.Unlock()
	return s.ws.WriteJSON(v)
}

func Initialize(create bool, code string, onChannelOpen func(dc *webrtc.DataChannel)) {
	apiURL := fmt.Sprintf("https://%s/burrow", viper.GetString("server"))
	wsURL := fmt.Sprintf("wss://%s/burrow", viper.GetString("server"))

	if create {
		var res map[string]string
		_, err := httpClient.R().
			SetResult(&res).
			Post(apiURL + "/session/new")
		if err != nil {
			log.Fatalf("Failed to create session: %v", err)
		}

		code = res["code"]
		fmt.Printf("[*] Session Created! Code: %s\n[*] Waiting for peer to join...\n", code)
	} else {
		fmt.Printf("[*] Joining session %s...\n", code)
	}

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{{URLs: []string{viper.GetString("stun")}}},
	}
	settingEngine := webrtc.SettingEngine{}
	settingEngine.SetNetworkTypes([]webrtc.NetworkType{
		webrtc.NetworkTypeUDP4,
		webrtc.NetworkTypeTCP4,
	})
	settingEngine.LoggerFactory = &logging.DefaultLoggerFactory{
		DefaultLogLevel: logging.LogLevelDebug,
		ScopeLevels: map[string]logging.LogLevel{
			"ice":  logging.LogLevelDebug,
			"dtls": logging.LogLevelDebug,
			"sctp": logging.LogLevelDebug,
		},
		Writer: os.Stderr,
	}
	settingEngine.SetIncludeLoopbackCandidate(false)

	// NOTE: Ensure viper.GetString("interface") matches the active interface
	// on BOTH the offering machine and the answering machine, otherwise gathering will fail.
	settingEngine.SetInterfaceFilter(func(iface string) bool {
		log.Printf("[*] ICE considering interface: %s", iface)
		return iface == viper.GetString("interface")
	})

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		log.Fatalf("Failed to create PeerConnection: %v", err)
	}

	pc.OnICEGatheringStateChange(func(state webrtc.ICEGathererState) {
		log.Printf("[*] ICE Gathering State: %s", state.String())
	})

	wsEndpoint := fmt.Sprintf("%s/session/%s/ws", wsURL, code)
	dialer := websocket.DefaultDialer
	header := http.Header{}

	header.Add("Origin", "https://"+viper.GetString("server"))
	header.Add("User-Agent", "Burrow 0.1")

	ws, resp, err := dialer.Dial(wsEndpoint, header)
	if err != nil {
		if resp != nil {
			log.Fatalf("Failed to connect to websocket. Server Handshake Status: %s", resp.Status)
		}
		log.Fatalf("Failed to connect to websocket: %v", err)
	}

	session := &PeerSession{pc: pc, ws: ws}

	// We no longer send Trickle ICE candidates over the WebSocket here.
	// Instead, we wait for gathering to finish and send the SDP with all candidates baked in.
	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			log.Printf("[*] Gathered candidate locally: %s", c.String())
		} else {
			log.Printf("[*] ICE gathering complete (nil candidate)")
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		fmt.Printf("[*] P2P State changed: %s\n", state.String())
		if state == webrtc.PeerConnectionStateConnected {
			ws.Close()
			return
		}
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			fmt.Println("Connection lost. Exiting.")
			os.Exit(0)
		}
	})

	go func() {
		for {
			_, msgBytes, err := ws.ReadMessage()
			if err != nil {
				return
			}

			var raw map[string]any
			if err := json.Unmarshal(msgBytes, &raw); err != nil {
				continue
			}

			if msgType, ok := raw["type"].(string); ok {
				if msgType == "role" {
					role := raw["role"].(string)
					session.handleRole(role, onChannelOpen)
					continue
				}
				if msgType == "error" {
					log.Fatalf("Backend Error: %v", raw["error"])
				}
			}

			if msgType, ok := raw["type"].(string); ok && (msgType == "offer" || msgType == "answer") {
				var sdp webrtc.SessionDescription
				json.Unmarshal(msgBytes, &sdp)
				if err := pc.SetRemoteDescription(sdp); err != nil {
					log.Printf("[!] SetRemoteDescription failed: %v", err)
					continue
				}

				if msgType == "offer" {
					answer, err := pc.CreateAnswer(nil)
					if err != nil {
						log.Printf("[!] CreateAnswer failed: %v", err)
						continue
					}

					// Spawn a goroutine to prevent blocking the WebSocket read loop
					go func() {
						gatherComplete := webrtc.GatheringCompletePromise(pc)
						pc.SetLocalDescription(answer)

						// Block until ICE gathering is completely finished
						<-gatherComplete

						// Send the Answer containing all ICE candidates
						session.writeWS(*pc.LocalDescription())
					}()
				}
			}

			// Left intact just in case the remote peer decides to trickle anyway
			if _, ok := raw["candidate"]; ok {
				var candidate webrtc.ICECandidateInit
				json.Unmarshal(msgBytes, &candidate)
				log.Printf("[*] Received remote trickle candidate: %s", candidate.Candidate)
				pc.AddICECandidate(candidate)
			}
		}
	}()

	select {} // Block forever
}

func (s *PeerSession) handleRole(role string, onChannelOpen func(dc *webrtc.DataChannel)) {
	if role == "offer" {
		dc, err := s.pc.CreateDataChannel("fileTransfer", nil)
		if err != nil {
			log.Fatalf("Failed to create data channel: %v", err)
		}
		dc.OnOpen(func() { onChannelOpen(dc) })

		offer, err := s.pc.CreateOffer(nil)
		if err != nil {
			log.Fatalf("Failed to create offer: %v", err)
		}

		// Spawn a goroutine to wait for gathering completion before sending
		go func() {
			gatherComplete := webrtc.GatheringCompletePromise(s.pc)
			s.pc.SetLocalDescription(offer)

			// Block until ICE gathering is completely finished
			<-gatherComplete

			// Send the Offer containing all ICE candidates
			s.writeWS(*s.pc.LocalDescription())
		}()
	} else {
		s.pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnOpen(func() { onChannelOpen(dc) })
		})
	}
}
