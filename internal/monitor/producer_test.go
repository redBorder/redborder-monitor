package monitor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
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
	done := make(chan struct{})

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
		close(done)
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
	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for HTTP request")
	}

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
			doneSub := make(chan struct{})

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reqContentEncoding = r.Header.Get("Content-Encoding")
				var err error
				reqBody, err = io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				w.WriteHeader(http.StatusOK)
				close(doneSub)
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
			select {
			case <-doneSub:
			case <-time.After(1 * time.Second):
				t.Fatal("timed out waiting for HTTP request")
			}

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

func TestMetricProducer_SendHTTP_Batching_SizeTrigger(t *testing.T) {
	receivedBatches := make(chan []Metric, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var batch []Metric
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("failed to unmarshal JSON array: %v", err)
			http.Error(w, err.Error(), 400)
			return
		}
		receivedBatches <- batch
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 3,
		},
	}

	producer := NewMetricProducer(conf)
	defer producer.Close()

	producer.Produce(Metric{Timestamp: 1, Monitor: "m1", Value: "10"})
	producer.Produce(Metric{Timestamp: 2, Monitor: "m2", Value: "20"})
	producer.Produce(Metric{Timestamp: 3, Monitor: "m3", Value: "30"})

	select {
	case batch := <-receivedBatches:
		if len(batch) != 3 {
			t.Fatalf("expected batch of size 3, got %d", len(batch))
		}
		if batch[0].Monitor != "m1" || batch[1].Monitor != "m2" || batch[2].Monitor != "m3" {
			t.Errorf("unexpected batch contents: %+v", batch)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for size-triggered batch")
	}
}

func TestMetricProducer_SendHTTP_Batching_TimerTrigger(t *testing.T) {
	receivedBatches := make(chan []Metric, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var batch []Metric
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("failed to unmarshal JSON array: %v", err)
			http.Error(w, err.Error(), 400)
			return
		}
		receivedBatches <- batch
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 10,
		},
	}

	producer := NewMetricProducer(conf)
	defer producer.Close()

	producer.Produce(Metric{Timestamp: 10, Monitor: "timer_m1", Value: "100"})

	select {
	case batch := <-receivedBatches:
		if len(batch) != 1 {
			t.Fatalf("expected batch of size 1 from timer, got %d", len(batch))
		}
		if batch[0].Monitor != "timer_m1" {
			t.Errorf("unexpected monitor: %s", batch[0].Monitor)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for timer-triggered batch")
	}
}

func TestMetricProducer_SendHTTP_Batching_CustomFlushTimer(t *testing.T) {
	receivedBatches := make(chan []Metric, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var batch []Metric
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("failed to unmarshal JSON array: %v", err)
			http.Error(w, err.Error(), 400)
			return
		}
		receivedBatches <- batch
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 10,
			RbHttpFlushTimer:  2,
		},
	}

	producer := NewMetricProducer(conf)
	defer producer.Close()

	start := time.Now()
	producer.Produce(Metric{Timestamp: 10, Monitor: "custom_timer_m1", Value: "100"})

	select {
	case batch := <-receivedBatches:
		elapsed := time.Since(start)
		if elapsed < 1800*time.Millisecond {
			t.Fatalf("batch was flushed too early (%v), expected >= 1.8s", elapsed)
		}
		if len(batch) != 1 {
			t.Fatalf("expected batch of size 1 from timer, got %d", len(batch))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for custom timer flush")
	}
}

func TestMetricProducer_SendHTTP_Batching_CloseFlush(t *testing.T) {
	receivedBatches := make(chan []Metric, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var batch []Metric
		if err := json.Unmarshal(body, &batch); err != nil {
			t.Errorf("failed to unmarshal JSON array: %v", err)
			http.Error(w, err.Error(), 400)
			return
		}
		receivedBatches <- batch
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 100, // Large batch size
		},
	}

	producer := NewMetricProducer(conf)
	producer.Produce(Metric{Timestamp: 20, Monitor: "close_m1", Value: "200"})
	producer.Produce(Metric{Timestamp: 21, Monitor: "close_m2", Value: "201"})

	// Immediate close must flush remaining 2 items
	producer.Close()

	select {
	case batch := <-receivedBatches:
		if len(batch) != 2 {
			t.Fatalf("expected flushed batch of size 2 on Close(), got %d", len(batch))
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for Close() flush")
	}
}

func TestMetricProducer_SendHTTP_Batching_Compression(t *testing.T) {
	modes := []string{"gzip", "deflated"}

	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			var receivedData []byte
			var reqEncoding string
			done := make(chan struct{})

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reqEncoding = r.Header.Get("Content-Encoding")
				var err error
				receivedData, err = io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				w.WriteHeader(http.StatusOK)
				close(done)
			}))
			defer server.Close()

			conf := &Config{
				Conf: Conf{
					Stdout:            0,
					HttpEndpoint:      server.URL,
					RbHttpMode:        mode,
					RbHttpMaxMessages: 2,
				},
			}

			producer := NewMetricProducer(conf)
			defer producer.Close()

			producer.Produce(Metric{Timestamp: 101, Monitor: "comp1", Value: "1.1"})
			producer.Produce(Metric{Timestamp: 102, Monitor: "comp2", Value: "2.2"})

			select {
			case <-done:
			case <-time.After(1 * time.Second):
				t.Fatal("timed out waiting for compressed batch")
			}

			expectedEncoding := mode
			if mode == "deflated" {
				expectedEncoding = "deflate"
			}
			if reqEncoding != expectedEncoding {
				t.Errorf("expected Content-Encoding %s, got %s", expectedEncoding, reqEncoding)
			}

			var decompressed []byte
			if mode == "gzip" {
				r, err := gzip.NewReader(bytes.NewReader(receivedData))
				if err != nil {
					t.Fatalf("gzip reader failed: %v", err)
				}
				decompressed, _ = io.ReadAll(r)
				_ = r.Close()
			} else {
				r := flate.NewReader(bytes.NewReader(receivedData))
				decompressed, _ = io.ReadAll(r)
				_ = r.Close()
			}

			var batch []Metric
			if err := json.Unmarshal(decompressed, &batch); err != nil {
				t.Fatalf("failed to unmarshal decompressed batch: %v", err)
			}
			if len(batch) != 2 || batch[0].Monitor != "comp1" || batch[1].Monitor != "comp2" {
				t.Errorf("unexpected batch content: %+v", batch)
			}
		})
	}
}

func TestMetricProducer_SendHTTP_Batching_UpdateConfig(t *testing.T) {
	receivedBatches := make(chan []Metric, 10)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		var batch []Metric
		if err := json.Unmarshal(body, &batch); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		receivedBatches <- batch
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	confInitial := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 5,
		},
	}

	producer := NewMetricProducer(confInitial)
	defer producer.Close()

	// Update to batch size of 2
	confUpdated := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 2,
		},
	}
	producer.UpdateConfig(confUpdated)

	producer.Produce(Metric{Timestamp: 301, Monitor: "upd1", Value: "1"})
	producer.Produce(Metric{Timestamp: 302, Monitor: "upd2", Value: "2"})

	select {
	case batch := <-receivedBatches:
		if len(batch) != 2 {
			t.Fatalf("expected batch of size 2 after config update, got %d", len(batch))
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch after UpdateConfig")
	}

	// Update to flush timer of 2s
	confTimerUpdated := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 5,
			RbHttpFlushTimer:  2,
		},
	}
	producer.UpdateConfig(confTimerUpdated)

	start := time.Now()
	producer.Produce(Metric{Timestamp: 303, Monitor: "upd3", Value: "3"})

	select {
	case batch := <-receivedBatches:
		elapsed := time.Since(start)
		if elapsed < 1800*time.Millisecond {
			t.Fatalf("batch was flushed too early (%v), expected >= 1.8s", elapsed)
		}
		if len(batch) != 1 {
			t.Fatalf("expected batch of size 1 after timer config update, got %d", len(batch))
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for batch after flush timer UpdateConfig")
	}
}

func TestMetricProducer_Produce_AfterClose(t *testing.T) {
	received := make(chan []byte, 10)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 5,
		},
	}

	producer := NewMetricProducer(conf)
	producer.Close()

	// Calling Produce after Close()
	producer.Produce(Metric{Timestamp: 999, Monitor: "after_close", Value: "1"})

	select {
	case body := <-received:
		t.Fatalf("BUG: Produce after Close() still sent HTTP request: %s", string(body))
	case <-time.After(200 * time.Millisecond):
		// Expected: no HTTP request after Close
	}
}

func TestMetricProducer_Produce_BackpressureBlocks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 2, // buffer = 20
		},
	}

	producer := NewMetricProducer(conf)
	defer producer.Close()

	done := make(chan struct{})
	go func() {
		// Send 25 metrics
		for i := 0; i < 25; i++ {
			producer.Produce(Metric{Timestamp: int64(i), Monitor: "backpressure", Value: "1"})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Logf("Produce() blocked by HTTP backpressure (channel capacity 20 reached)")
	}
}

func TestMetricProducer_SendHTTP_Batching_ConcurrentProduceAndUpdate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conf := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 5,
		},
	}

	producer := NewMetricProducer(conf)

	var wg sync.WaitGroup
	// 5 concurrent producers
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				producer.Produce(Metric{Timestamp: int64(j), Monitor: "race_test", Value: "1"})
				time.Sleep(1 * time.Millisecond)
			}
		}(i)
	}

	// 1 concurrent reconfigurer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 2; i <= 6; i++ {
			producer.UpdateConfig(&Config{
				Conf: Conf{
					Stdout:            0,
					HttpEndpoint:      server.URL,
					RbHttpMode:        "normal",
					RbHttpMaxMessages: i,
				},
			})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()
	producer.Close()
}

func TestMetricProducer_SendHTTP_SwitchBatchAndSingle(t *testing.T) {
	received := make(chan string, 20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// 1. Start in batch mode (batch size 2)
	confBatch := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 2,
		},
	}
	producer := NewMetricProducer(confBatch)

	producer.Produce(Metric{Timestamp: 1, Monitor: "b1", Value: "1"})
	producer.Produce(Metric{Timestamp: 2, Monitor: "b2", Value: "2"})

	select {
	case body := <-received:
		if body[0] != '[' {
			t.Fatalf("expected JSON array in batch mode, got: %s", body)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch")
	}

	// 2. Switch to single mode (batch size 1)
	confSingle := &Config{
		Conf: Conf{
			Stdout:            0,
			HttpEndpoint:      server.URL,
			RbHttpMode:        "normal",
			RbHttpMaxMessages: 1,
		},
	}
	producer.UpdateConfig(confSingle)

	producer.Produce(Metric{Timestamp: 3, Monitor: "s1", Value: "3"})

	select {
	case body := <-received:
		if body[0] != '{' {
			t.Fatalf("expected JSON object in single mode, got: %s", body)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for single metric")
	}

	// 3. Switch back to batch mode (batch size 2)
	producer.UpdateConfig(confBatch)

	producer.Produce(Metric{Timestamp: 4, Monitor: "b3", Value: "4"})
	producer.Produce(Metric{Timestamp: 5, Monitor: "b4", Value: "5"})

	select {
	case body := <-received:
		if body[0] != '[' {
			t.Fatalf("expected JSON array after switching back to batch mode, got: %s", body)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for batch after switchback")
	}

	producer.Close()
}




