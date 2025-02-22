package scheduler

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"

	"github.com/gosched/scheduler/pb"
	"golang.org/x/sync/errgroup"
	"storj.io/drpc/drpchttp"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
)

var emptyRes = &pb.Empty{}

// todo: add web sockets
// todo: add drpc

type Server struct {
	pb.DRPCSchedulerServerUnimplementedServer

	taskQue chan *Task
}

func (s *Server) Register(ctx context.Context, pbt *pb.Task) (*pb.Empty, error) {
	// todo:
	if pbt == nil {
		return emptyRes, errors.New("empty task")
	}

	var (
		at  time.Time
		err error
	)

	at, err = time.Parse(time.RFC3339, pbt.At)
	if err != nil {
		return emptyRes, err
	}

	t := &Task{
		Method:     pbt.Method,
		Parameters: pbt.Params,
		At:         at,
	}
	s.taskQue <- t
	return emptyRes, nil
}

func startServer(q chan *Task) error {
	s := &Server{
		taskQue: q,
	}

	m := drpcmux.New()
	err := pb.DRPCRegisterSchedulerServer(m, s)
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		return err
	}
	var (
		group errgroup.Group

		ctx = context.Background()

		lisMux  = drpcmigrate.NewListenMux(lis, len(drpcmigrate.DRPCHeader))
		drpcLis = lisMux.Route(drpcmigrate.DRPCHeader)
		httpLis = lisMux.Default()
	)
	group.Go(func() error {
		s := drpcserver.New(m)
		return s.Serve(ctx, drpcLis)
	})

	// http handling
	group.Go(func() error {
		s := http.Server{Handler: drpchttp.New(m)}
		return s.Serve(httpLis)
	})

	// run the listen mux
	group.Go(func() error {
		return lisMux.Run(ctx)
	})

	return group.Wait()
}
