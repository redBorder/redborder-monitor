package monitor

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type kafkaLogger struct {
	severity int
}

func (l kafkaLogger) Printf(format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	msg = strings.TrimSuffix(msg, "\n")
	LogMsg(l.severity, "Kafka: %s", msg)
}

// MetricProducer routes metrics to Stdout, Kafka, and HTTP.
type MetricProducer struct {
	mu          sync.RWMutex
	config      *Config
	kafkaWriter *kafka.Writer
	httpClient  *http.Client
	httpURL     string
	httpMode    string // "normal" or "deflated"
}

// NewMetricProducer initializes output endpoints based on configuration.
func NewMetricProducer(config *Config) *MetricProducer {
	p := &MetricProducer{
		config:   config,
		httpURL:  config.Conf.HttpEndpoint,
		httpMode: config.Conf.RbHttpMode,
	}

	// Initialize Kafka Writer if broker is specified
	if config.Conf.KafkaBroker != "" {
		broker := config.Conf.KafkaBroker
		// If port is not specified, default to 9092
		if !strings.Contains(broker, ":") {
			broker = broker + ":9092"
		}

		p.kafkaWriter = &kafka.Writer{
			Addr:        kafka.TCP(broker),
			Topic:       config.Conf.KafkaTopic,
			Balancer:    &kafka.LeastBytes{},
			Async:       true, // Non-blocking produce
			Logger:      kafkaLogger{severity: LogDebug},
			ErrorLogger: kafkaLogger{severity: LogErr},
		}

		if config.Conf.KafkaTimeout > 0 {
			p.kafkaWriter.WriteTimeout = time.Duration(config.Conf.KafkaTimeout) * time.Second
		} else {
			p.kafkaWriter.WriteTimeout = 5 * time.Second
		}
	}

	// Initialize HTTP Client if endpoint is specified
	if config.Conf.HttpEndpoint != "" {
		tlsConfig := &tls.Config{}
		if config.Conf.HttpInsecure == 1 {
			tlsConfig.InsecureSkipVerify = true
		}

		maxConns := config.Conf.HttpMaxTotalConnections
		if maxConns <= 0 {
			maxConns = 100
		}

		timeout := config.Conf.HttpTimeout
		if timeout <= 0 {
			timeout = 5
		}

		connTimeout := config.Conf.HttpConnTimeout
		if connTimeout <= 0 {
			connTimeout = 2
		}

		tr := &http.Transport{
			TLSClientConfig:     tlsConfig,
			MaxIdleConns:        maxConns,
			MaxIdleConnsPerHost: maxConns,
			IdleConnTimeout:     90 * time.Second,
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(connTimeout) * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		}

		p.httpClient = &http.Client{
			Transport: tr,
			Timeout:   time.Duration(timeout) * time.Second,
		}
	}

	return p
}

// Produce sends the metric to all enabled outputs.
func (p *MetricProducer) Produce(metric Metric) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	data, err := json.Marshal(metric)
	if err != nil {
		log.Printf("[ERROR] Failed to marshal metric: %v", err)
		return
	}

	// Stdout
	if p.config.Conf.Stdout == 1 {
		fmt.Println(string(data))
		IncMetricsSentStdout()
	}

	// Kafka
	if p.kafkaWriter != nil {
		err := p.kafkaWriter.WriteMessages(context.Background(), kafka.Message{
			Value: data,
		})
		if err != nil {
			log.Printf("[ERROR] Kafka write error: %v", err)
			IncMetricsFailedKafka()
		} else {
			IncMetricsSentKafka()
		}
	}

	// HTTP POST
	if p.httpClient != nil {
		go p.sendHTTP(data)
	}
}

func (p *MetricProducer) sendHTTP(data []byte) {
	p.mu.RLock()
	client := p.httpClient
	url := p.httpURL
	mode := p.httpMode
	p.mu.RUnlock()

	if client == nil {
		return
	}

	var body io.Reader = bytes.NewReader(data)
	var contentEncoding string

	if strings.ToLower(mode) == "deflated" {
		var buf bytes.Buffer
		w, err := flate.NewWriter(&buf, flate.BestCompression)
		if err == nil {
			_, _ = w.Write(data)
			_ = w.Close()
			body = &buf
			contentEncoding = "deflate"
		}
	} else if strings.ToLower(mode) == "gzip" {
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		_, _ = w.Write(data)
		_ = w.Close()
		body = &buf
		contentEncoding = "gzip"
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		log.Printf("[ERROR] Failed to create HTTP request: %v", err)
		IncMetricsFailedHTTP()
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("[ERROR] HTTP POST failed: %v", err)
		IncMetricsFailedHTTP()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		log.Printf("[ERROR] HTTP server returned status: %s", resp.Status)
		IncMetricsFailedHTTP()
	} else {
		IncMetricsSentHTTP()
	}
}

// UpdateConfig dynamically updates the output endpoints if relevant configuration changed.
func (p *MetricProducer) UpdateConfig(newConfig *Config) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Check if Kafka broker or topic changed
	kafkaChanged := p.config.Conf.KafkaBroker != newConfig.Conf.KafkaBroker ||
		p.config.Conf.KafkaTopic != newConfig.Conf.KafkaTopic ||
		p.config.Conf.KafkaTimeout != newConfig.Conf.KafkaTimeout

	// Check if HTTP endpoint or mode changed
	httpChanged := p.config.Conf.HttpEndpoint != newConfig.Conf.HttpEndpoint ||
		p.config.Conf.RbHttpMode != newConfig.Conf.RbHttpMode ||
		p.config.Conf.HttpInsecure != newConfig.Conf.HttpInsecure ||
		p.config.Conf.HttpMaxTotalConnections != newConfig.Conf.HttpMaxTotalConnections ||
		p.config.Conf.HttpTimeout != newConfig.Conf.HttpTimeout ||
		p.config.Conf.HttpConnTimeout != newConfig.Conf.HttpConnTimeout

	p.config = newConfig

	if kafkaChanged {
		if p.kafkaWriter != nil {
			_ = p.kafkaWriter.Close()
			p.kafkaWriter = nil
		}
		if newConfig.Conf.KafkaBroker != "" {
			broker := newConfig.Conf.KafkaBroker
			if !strings.Contains(broker, ":") {
				broker = broker + ":9092"
			}
			writer := &kafka.Writer{
				Addr:        kafka.TCP(broker),
				Topic:       newConfig.Conf.KafkaTopic,
				Balancer:    &kafka.LeastBytes{},
				Async:       true,
				Logger:      kafkaLogger{severity: LogDebug},
				ErrorLogger: kafkaLogger{severity: LogErr},
			}
			if newConfig.Conf.KafkaTimeout > 0 {
				writer.WriteTimeout = time.Duration(newConfig.Conf.KafkaTimeout) * time.Second
			} else {
				writer.WriteTimeout = 5 * time.Second
			}
			p.kafkaWriter = writer
			LogMsg(LogInfo, "Kafka producer updated: broker=%s, topic=%s", broker, newConfig.Conf.KafkaTopic)
		}
	}

	if httpChanged {
		p.httpClient = nil
		p.httpURL = newConfig.Conf.HttpEndpoint
		p.httpMode = newConfig.Conf.RbHttpMode

		if newConfig.Conf.HttpEndpoint != "" {
			tlsConfig := &tls.Config{}
			if newConfig.Conf.HttpInsecure == 1 {
				tlsConfig.InsecureSkipVerify = true
			}
			maxConns := newConfig.Conf.HttpMaxTotalConnections
			if maxConns <= 0 {
				maxConns = 100
			}
			timeout := newConfig.Conf.HttpTimeout
			if timeout <= 0 {
				timeout = 5
			}
			connTimeout := newConfig.Conf.HttpConnTimeout
			if connTimeout <= 0 {
				connTimeout = 2
			}
			tr := &http.Transport{
				TLSClientConfig:     tlsConfig,
				MaxIdleConns:        maxConns,
				MaxIdleConnsPerHost: maxConns,
				IdleConnTimeout:     90 * time.Second,
				DialContext: (&net.Dialer{
					Timeout:   time.Duration(connTimeout) * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
			}
			p.httpClient = &http.Client{
				Transport: tr,
				Timeout:   time.Duration(timeout) * time.Second,
			}
			LogMsg(LogInfo, "HTTP client updated: endpoint=%s, mode=%s", p.httpURL, p.httpMode)
		}
	}
}

// Close flushes and closes active producers.
func (p *MetricProducer) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.kafkaWriter != nil {
		_ = p.kafkaWriter.Close()
	}
}
