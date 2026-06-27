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
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{viper.GetString("stun")}},
		},
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

	// If port_forward is enabled in config, bind to port 50000 only.
	// This means pion will advertise your-public-ip:50000 as a candidate,
	// which matches the port forward rule on your router.
	// Users without port forwarding leave this unset and pion picks a random
	// ephemeral port — which works fine for anyone on Cone NAT.
	if viper.GetBool("port_forward") {
    if err := settingEngine.SetEphemeralUDPPortRange(50000, 50005); err != nil {
        log.Fatalf("Failed to set UDP port range: %v", err)
    }
    // Filter to configured interface to avoid port conflicts across interfaces
    if iface := viper.GetString("interface"); iface != "" {
        settingEngine.SetInterfaceFilter(func(i string) bool {
            return i == iface
        })
        log.Printf("[*] Port forwarding mode enabled: binding to UDP 50000-50005 on %s", iface)
    } else {
        log.Printf("[*] Port forwarding mode enabled: binding to UDP 50000-50005")
    }
	}

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

	pc.OnICECandidate(func(c *webrtc.ICECandidate) {
		if c != nil {
			log.Printf("[*] Gathered candidate locally: %s", c.String())
		}
	})

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		fmt.Printf("[*] P2P State changed: %s\n", state.String())
		if state == webrtc.PeerConnectionStateConnected {
			ws.Close()
			return
		}
		if state == webrtc.PeerConnectionStateFailed {
			fmt.Println("[!] Connection failed. This is likely due to Symmetric NAT on one or both ends.")
			fmt.Println("[!] Workarounds: forward UDP port 50000 on your router, or use a VPN like Tailscale.")
			os.Exit(1)
		}
		if state == webrtc.PeerConnectionStateClosed {
			fmt.Println("Connection closed. Exiting.")
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

				log.Printf("[*] SUCCESS: Received remote %s from signaling server!", msgType)

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

					go func() {
						gatherComplete := webrtc.GatheringCompletePromise(pc)
						if err := pc.SetLocalDescription(answer); err != nil {
							log.Printf("[!] SetLocalDescription failed: %v", err)
							return
						}

						<-gatherComplete

						log.Printf("[*] Sending Answer to peer...")
						session.writeWS(*pc.LocalDescription())
					}()
				}
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

		offer, err := s.pc.CreateOffer(nil)
		if err != nil {
			log.Fatalf("Failed to create offer: %v", err)
		}

		go func() {
			gatherComplete := webrtc.GatheringCompletePromise(s.pc)
			if err := s.pc.SetLocalDescription(offer); err != nil {
				log.Printf("[!] SetLocalDescription failed: %v", err)
				return
			}

			<-gatherComplete

			log.Printf("[*] Sending Offer to peer...")
			s.writeWS(*s.pc.LocalDescription())
		}()
	} else {
		s.pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			dc.OnOpen(func() { onChannelOpen(dc) })
		})
	}
}
