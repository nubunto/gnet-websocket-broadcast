package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/gobwas/ws/wsutil"
	"github.com/panjf2000/gnet/v2"
	"github.com/panjf2000/gnet/v2/pkg/logging"
)

type wsServer struct {
	gnet.BuiltinEventEngine

	addr                      string
	atomicNumberOfConnections int64

	bs *broadcastService
}

type broadcastService struct {
	connections map[gnet.Conn]struct{}
}

func (b *broadcastService) broadcastMessage(op ws.OpCode, msg []byte) error {
	for c, _ := range b.connections {
		err := wsutil.WriteServerMessage(c, op, msg)
		if err != nil {
			return fmt.Errorf("writing server message: %w", err)
		}
	}
	return nil
}

func (b *broadcastService) trackConnection(c gnet.Conn) {
	b.connections[c] = struct{}{}
}

func (b *broadcastService) untrackConnection(c gnet.Conn) {
	delete(b.connections, c)
}

type wsCodec struct {
	upgradedWebsocketConnection bool
}

func (wss *wsServer) OnBoot(eng gnet.Engine) gnet.Action {
	logging.Infof("echo server with multi-core=true is listening on %s", wss.addr)

	return gnet.None
}

func (wss *wsServer) OnOpen(conn gnet.Conn) ([]byte, gnet.Action) {
	conn.SetContext(new(wsCodec))

	atomic.AddInt64(&wss.atomicNumberOfConnections, 1)

	wss.bs.trackConnection(conn)

	return nil, gnet.None
}

func (wss *wsServer) OnClose(conn gnet.Conn, err error) gnet.Action {
	if err != nil {
		logging.Warnf("error occurred on connection=%s, %v\n", conn.RemoteAddr().String(), err)
	}

	atomic.AddInt64(&wss.atomicNumberOfConnections, -1)
	logging.Infof("conn[%v] disconnected", conn.RemoteAddr().String())

	wss.bs.untrackConnection(conn)

	return gnet.None
}

func (wss *wsServer) OnTraffic(conn gnet.Conn) gnet.Action {
	codec, ok := conn.Context().(*wsCodec)
	if !ok {
		logging.Errorf("unexpected context type, shutting down connection")

		return gnet.Close
	}

	if !codec.upgradedWebsocketConnection {
		logging.Infof("conn[%v] upgrade websocket protocol", conn.RemoteAddr().String())

		_, err := ws.Upgrade(conn)
		if err != nil {
			logging.Warnf("conn[%v] [err=%v]", conn.RemoteAddr().String(), err.Error())

			return gnet.Close
		}

		codec.upgradedWebsocketConnection = true

		return gnet.None
	}

	msg, op, err := wsutil.ReadClientData(conn)
	if err != nil {
		if _, ok := err.(wsutil.ClosedError); !ok {
			logging.Warnf("conn[%v] [err=%v]", conn.RemoteAddr().String(), err.Error())
		}

		return gnet.Close
	}

	logging.Infof("conn[%v] receive [op=%v] [msg=%v]", conn.RemoteAddr().String(), op, string(msg))

	err = wss.bs.broadcastMessage(op, msg)

	if err != nil {
		logging.Warnf("conn[%v] [err=%v]", conn.RemoteAddr().String(), err.Error())

		return gnet.Close
	}

	return gnet.None
}

func (wss *wsServer) OnTick() (time.Duration, gnet.Action) {
	logging.Infof("[connected-count=%v]", atomic.LoadInt64(&wss.atomicNumberOfConnections))

	wss.bs.broadcastMessage(ws.OpText, []byte("system: This is a broadcasted system message!"))

	return 3 * time.Second, gnet.None
}

func main() {
	var port int

	flag.IntVar(&port, "port", 9000, "server port")
	flag.Parse()

	bs := &broadcastService{
		connections: make(map[gnet.Conn]struct{}),
	}

	wss := &wsServer{
		addr: fmt.Sprintf("tcp://0.0.0.0:%d", port),
		bs:   bs,
	}

	log.Println(
		"server exits:",
		gnet.Run(
			wss,
			wss.addr,
			gnet.WithMulticore(true),
			gnet.WithReusePort(true),
			gnet.WithTicker(true),
		),
	)
}
