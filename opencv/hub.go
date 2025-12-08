package main

import (
	"flag"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Consumer struct {
	conn *websocket.Conn
	send chan []byte
}

type Producer struct {
	id        string
	consumers map[*Consumer]struct{}
}

type Hub struct {
	mu       sync.RWMutex
	producers map[string]*Producer
}

func NewHub() *Hub {
	return &Hub{producers: make(map[string]*Producer)}
}

func (h *Hub) registerProducer(id string, conn *websocket.Conn) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.producers[id]; exists {
		return http.ErrUseLastResponse
	}
	h.producers[id] = &Producer{id: id, consumers: make(map[*Consumer]struct{})}
	return nil
}

func (h *Hub) unregisterProducer(id string) {
	h.mu.Lock()
	prod, ok := h.producers[id]
	if ok {
		delete(h.producers, id)
	}
	h.mu.Unlock()
	if !ok {
		return
	}
	for consumer := range prod.consumers {
		consumer.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "Producer disconnected"), time.Now().Add(time.Second))
		consumer.conn.Close()
	}
}

func (h *Hub) addConsumer(id string, c *Consumer) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	prod, ok := h.producers[id]
	if !ok {
		return false
	}
	prod.consumers[c] = struct{}{}
	return true
}

func (h *Hub) removeConsumer(id string, c *Consumer) {
	h.mu.Lock()
	if prod, ok := h.producers[id]; ok {
		delete(prod.consumers, c)
	}
	h.mu.Unlock()
	select {
	case <-c.send:
	default:
	}
	close(c.send)
}

func (h *Hub) broadcast(id string, frame []byte) {
	h.mu.RLock()
	prod, ok := h.producers[id]
	if !ok {
		h.mu.RUnlock()
		return
	}
	consumers := make([]*Consumer, 0, len(prod.consumers))
	for c := range prod.consumers {
		consumers = append(consumers, c)
	}
	h.mu.RUnlock()
	for _, c := range consumers {
		select {
		case c.send <- frame:
		default:
			select {
			case <-c.send:
			default:
			}
			c.send <- frame
		}
	}
}

func producerHandler(h *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		if err := h.registerProducer(id, conn); err != nil {
			conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "ID in use"), time.Now().Add(time.Second))
			conn.Close()
			return
		}
		defer h.unregisterProducer(id)
		for {
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				break
			}
			if messageType == websocket.BinaryMessage {
				h.broadcast(id, data)
			}
		}
	}
}

func consumerHandler(h *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.URL.Query().Get("id")
		if id == "" {
			http.Error(w, "missing id", http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		consumer := &Consumer{conn: conn, send: make(chan []byte, 4)}
		if ok := h.addConsumer(id, consumer); !ok {
			conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, "Producer not found"), time.Now().Add(time.Second))
			conn.Close()
			return
		}
		go writePump(h, id, consumer)
		readPump(h, id, consumer)
	}
}

func writePump(h *Hub, producerID string, c *Consumer) {
	for frame := range c.send {
		if err := c.conn.WriteMessage(websocket.BinaryMessage, frame); err != nil {
			break
		}
	}
}

func readPump(h *Hub, producerID string, c *Consumer) {
	defer h.removeConsumer(producerID, c)
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			break
		}
	}
}

func main() {
	addr := flag.String("addr", ":9000", "HTTP server address")
	flag.Parse()

	h := NewHub()
	http.HandleFunc("/ws/producer", producerHandler(h))
	http.HandleFunc("/ws/consumer", consumerHandler(h))

	log.Printf("WebSocket hub listening on %s", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
