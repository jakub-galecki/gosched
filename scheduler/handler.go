package scheduler

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
)

type Handler interface {
	Handle(*Task) error
}

func NewHttpHandler(addr string, logPath string) *HttpHandler {
	out, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil
	}
	l := slog.New(slog.NewJSONHandler(out, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	return &HttpHandler{
		addr:   addr,
		logger: l,
	}
}

type HttpHandler struct {
	addr   string
	url    url.URL
	logger *slog.Logger
}

func (h *HttpHandler) Handle(t *Task) error {
	uri, err := url.JoinPath(h.addr, t.Method)
	if err != nil {
		return err
	}

	h.logger.Debug("handling task",
		slog.String("method", t.Method),
		slog.Any("params", t.Parameters))

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	for k, v := range t.Parameters {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()
	reqUrl := req.URL.String()
	res, err := http.Get(reqUrl)
	if err != nil {
		return err
	}

	h.logger.Debug("sending message", slog.String("uri", reqUrl))

	if res.StatusCode > 201 {
		body, _ := io.ReadAll(res.Body)
		h.logger.Error("invalid status code",
			slog.Int("status_code", res.StatusCode),
			slog.String("response", string(body)))
		return errors.New("invalid status code")
	}

	resp, err := io.ReadAll(res.Body)
	if err != nil {
		h.logger.Error("couldnt read whole response body")
		return err
	}

	// todo: maybe notify sender if option was provided
	slog.Debug("got response", slog.String("body", string(resp)))
	return nil
}
