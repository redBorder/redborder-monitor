package monitor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestMetricProducer_SendHTTP_Success(t *testing.T) {
	metricData := Metric{
		Timestamp: 123456,
		Monitor:   "test_http_monitor",
		Value:     "100.000000",
		GroupID:   int64(99),
	}

	var reqBody []byte
	var reqContentType string
	var reqContentEncoding string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqContentType = r.Header.Get("Content-Type")
		reqContentEncoding = r.Header.Get("Content-Encoding")
		var err error
		reqBody, err = io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:       0,
			HttpEndpoint: server.URL,
			RbHttpMode:   "normal",
		},
	}

	producer := NewMetricProducer(conf)
	defer producer.Close()

	producer.Produce(metricData)
	time.Sleep(100 * time.Millisecond)

	if reqContentType != "application/json" {
		t.Errorf("expected Content-Type 'application/json', got '%s'", reqContentType)
	}
	if reqContentEncoding != "" {
		t.Errorf("expected empty Content-Encoding, got '%s'", reqContentEncoding)
	}

	var received Metric
	err := json.Unmarshal(reqBody, &received)
	if err != nil {
		t.Fatalf("failed to unmarshal request body: %v", err)
	}

	if received.Monitor != metricData.Monitor || received.Value != metricData.Value || received.Timestamp != metricData.Timestamp {
		t.Errorf("received metric does not match: %+v vs %+v", received, metricData)
	}
}

func TestMetricProducer_SendHTTP_GzipAndDeflate(t *testing.T) {
	modes := []string{"gzip", "deflated"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			metricData := Metric{
				Timestamp: 987654,
				Monitor:   "comp_monitor",
				Value:     "77.700000",
			}

			var reqBody []byte
			var reqContentEncoding string

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reqContentEncoding = r.Header.Get("Content-Encoding")
				var err error
				reqBody, err = io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			conf := &Config{
				Conf: Conf{
					Stdout:       0,
					HttpEndpoint: server.URL,
					RbHttpMode:   mode,
				},
			}

			producer := NewMetricProducer(conf)
			defer producer.Close()

			producer.Produce(metricData)
			time.Sleep(100 * time.Millisecond)

			if reqContentEncoding != mode {
				expectedEncoding := mode
				if mode == "deflated" {
					expectedEncoding = "deflate"
				}
				if reqContentEncoding != expectedEncoding {
					t.Errorf("expected Content-Encoding '%s', got '%s'", expectedEncoding, reqContentEncoding)
				}
			}

			var decompressed []byte
			if mode == "gzip" {
				r, err := gzip.NewReader(bytes.NewReader(reqBody))
				if err != nil {
					t.Fatalf("gzip reader creation failed: %v", err)
				}
				decompressed, err = io.ReadAll(r)
				if err != nil {
					t.Fatalf("failed to read gzipped body: %v", err)
				}
				r.Close()
			} else {
				r := flate.NewReader(bytes.NewReader(reqBody))
				var err error
				decompressed, err = io.ReadAll(r)
				if err != nil {
					t.Fatalf("failed to read deflated body: %v", err)
				}
				r.Close()
			}

			var received Metric
			err := json.Unmarshal(decompressed, &received)
			if err != nil {
				t.Fatalf("failed to unmarshal decompressed json: %v", err)
			}

			if received.Monitor != metricData.Monitor {
				t.Errorf("expected monitor '%s', got '%s'", metricData.Monitor, received.Monitor)
			}
		})
	}
}

func TestMetricProducer_SendHTTP_Failure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:       0,
			HttpEndpoint: server.URL,
			RbHttpMode:   "normal",
		},
	}

	producer := NewMetricProducer(conf)
	defer producer.Close()

	producer.Produce(Metric{Timestamp: 111, Monitor: "test_fail", Value: "0"})
	time.Sleep(100 * time.Millisecond)

	confInvalid := &Config{
		Conf: Conf{
			Stdout:       0,
			HttpEndpoint: "http://127.0.0.1:55443/invalid-endpoint-path",
			RbHttpMode:   "normal",
		},
	}

	producerInvalid := NewMetricProducer(confInvalid)
	defer producerInvalid.Close()

	producerInvalid.Produce(Metric{Timestamp: 222, Monitor: "test_fail_conn", Value: "0"})
	time.Sleep(100 * time.Millisecond)
}
