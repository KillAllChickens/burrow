package conn

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v3"
	"github.com/spf13/viper"
	"resty.dev/v3"
)

var httpClient *resty.Client = resty.New().SetHeader("User-Agent", "Burrow 0.1")

var wsMutex sync.Mutex

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

	api := webrtc.NewAPI(webrtc.WithSettingEngine(settingEngine))

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		log.Fatalf("Failed to create PeerConnection: %v", err)
	}

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

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			session.writeWS(c.ToJSON())
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
				pc.SetRemoteDescription(sdp)

				if msgType == "offer" {
					answer, _ := pc.CreateAnswer(nil)
					pc.SetLocalDescription(answer)
					session.writeWS(answer)
				}
			}
			if _, ok := raw["candidate"]; ok {
				var candidate webrtc.ICECandidateInit
				json.Unmarshal(msgBytes, &candidate)
				pc.AddICECandidate(candidate)
			}
		}
	}()

	select {}
}

func (s *PeerSession) handleRole(role string, onChannelOpen func(dc *webrtc.DataChannel)) {
	if role == "offer" {
		dc, err := s.pc.CreateDataChannel("fileTransfer", nil)
		if err != nil {
			log.Fatalf("Failed to create data channel: %v", err)
		}
		dc.OnOpen(func() { onChannelOpen(dc) })

		offer, _ := s.pc.CreateOffer(nil)
		s.pc.SetLocalDescription(offer)
		s.writeWS(offer)
	} else {
		s.pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnOpen(func() { onChannelOpen(dc) })
		})
	}
}
