package scheduler

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
)

type Handler interface {
	Handle(*Task) error
}

func NewHttpHandler(addr string, logger *slog.Logger) *HttpHandler {
	return &HttpHandler{
		addr:   addr,
		logger: logger,
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
		slog.String("params", t.Parameters))

	req, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Set("params", t.Parameters)
	req.URL.RawQuery = q.Encode()
	reqUrl := req.URL.String()
	res, err := http.Get(reqUrl)
	if err != nil {
		return err
	}

	h.logger.Debug("sending message", slog.String("uri", reqUrl))

	if res.StatusCode != 200 {
		h.logger.Error("invalid status code", slog.Int("status_code", res.StatusCode))
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
