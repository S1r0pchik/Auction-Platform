package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func (a *API) ws(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	// Hold capacity until the handler exits, even when the client is too slow.
	sub, unsubscribe, err := a.Hub.Subscribe(id)
	if err != nil {
		cancel()
		w.Header().Set("Retry-After", "1")
		writeError(w, r, 429, "websocket_capacity_exceeded")
		return
	}
	defer unsubscribe()
	_, err = a.Auctions.Get(ctx, id)
	cancel()
	if err != nil {
		fail(w, r, err)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1024)
	readCtx := conn.CloseRead(r.Context())
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-readCtx.Done():
			return
		case <-sub.Done:
			conn.Close(websocket.StatusPolicyViolation, "slow client; resync using REST")
			return
		case e := <-sub.Events:
			writeCtx, cancel := context.WithTimeout(readCtx, 5*time.Second)
			err = wsjson.Write(writeCtx, conn, e)
			cancel()
			if err != nil {
				return
			}
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(readCtx, 5*time.Second)
			err = conn.Ping(pingCtx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}
