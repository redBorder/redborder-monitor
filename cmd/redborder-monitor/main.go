package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"redborder-monitor/internal/monitor"
)

var (
	Version        = "dev"
	configPath     string
	daemonMode     bool
	showHelp       bool
	showVersion    bool
	showInfo       bool
	listMibs       bool
	searchOid      string
	showStatus     bool
	resetStatsFlag bool
	debugLevel     int
	config         *monitor.Config
	sensorsMu      sync.RWMutex
	safeSensors    []*monitor.SafeSensor
	producer       *monitor.MetricProducer
	ipcListener    net.Listener
	ipcListenerMu  sync.Mutex
)

func init() {
	// Short flags
	flag.StringVar(&configPath, "c", "", "Path to configuration file")
	flag.IntVar(&debugLevel, "d", 100, "Debug severity level")
	flag.BoolVar(&daemonMode, "g", false, "Go Daemon mode (run in background)")
	flag.BoolVar(&showHelp, "h", false, "Show help")
	flag.BoolVar(&showInfo, "i", false, "Show available plugins and configuration JSON fields")
	flag.BoolVar(&listMibs, "l", false, "List all loaded MIB modules and exit")
	flag.BoolVar(&resetStatsFlag, "r", false, "Reset daemon stats counters and exit")
	flag.StringVar(&searchOid, "s", "", "Search for loaded MIB objects by name (case-insensitive)")
	flag.BoolVar(&showStatus, "t", false, "Show daemon status/stats and exit")
	flag.BoolVar(&showVersion, "v", false, "Show version")

	// Long flags
	flag.StringVar(&configPath, "config", "", "Path to configuration file")
	flag.IntVar(&debugLevel, "debug", 100, "Debug severity level")
	flag.BoolVar(&daemonMode, "daemon", false, "Go Daemon mode (run in background)")
	flag.BoolVar(&showHelp, "help", false, "Show help")
	flag.BoolVar(&showInfo, "info", false, "Show available plugins and configuration JSON fields")
	flag.BoolVar(&listMibs, "list-mibs", false, "List all loaded MIB modules and exit")
	flag.BoolVar(&resetStatsFlag, "reset", false, "Reset daemon stats counters and exit")
	flag.StringVar(&searchOid, "search-mibs", "", "Search for loaded MIB objects by name (case-insensitive)")
	flag.BoolVar(&showStatus, "status", false, "Show daemon status/stats and exit")
	flag.BoolVar(&showVersion, "version", false, "Show version")

	flag.Usage = printHelp
}

func main() {
	flag.Parse()

	if showHelp {
		printHelp()
		return
	}

	if showVersion {
		fmt.Printf("redborder-monitorversion %s\n", Version)
		fmt.Println("Owner: Eneo Tecnología SL.")
		fmt.Println("Author: dvanhoucke@redborder.com")
		fmt.Println("License: GNU AGPLv3")
		return
	}

	if showInfo {
		monitor.PrintInfo(os.Stdout)
		return
	}

	if listMibs {
		listMIBModules()
		return
	}

	if searchOid != "" {
		searchMIBObjects(searchOid)
		return
	}

	if showStatus {
		queryStatus(configPath)
		return
	}

	if resetStatsFlag {
		sendResetCommand(configPath)
		return
	}

	if configPath == "" {
		monitor.LogMsg(monitor.LogErr, "Config path (-c) is required.")
		printHelp()
		os.Exit(1)
	}

	// Load configuration
	var err error
	config, err = monitor.LoadConfig(configPath)
	if err != nil {
		monitor.LogMsg(monitor.LogCrit, "Failed to load configuration: %v", err)
		os.Exit(1)
	}

	if config.Conf.MaxSimultaneousQueries > 0 {
		monitor.MaxSimultaneousQueries = config.Conf.MaxSimultaneousQueries
	} else {
		monitor.MaxSimultaneousQueries = 10
	}

	if config.Conf.Debug > 0 {
		atomic.StoreInt32(&monitor.AtomicDebugLevel, int32(config.Conf.Debug))
	} else {
		atomic.StoreInt32(&monitor.AtomicDebugLevel, int32(debugLevel))
	}

	monitor.LogMsg(monitor.LogInfo, "Starting redborder-monitor with debug level %d", atomic.LoadInt32(&monitor.AtomicDebugLevel))

	// Set up signal handling for clean shutdown and SIGHUP reload
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	go func() {
		for sig := range sigChan {
			if sig == syscall.SIGHUP {
				monitor.LogMsg(monitor.LogInfo, "Received SIGHUP. Reloading configuration...")
				reloadConfig(configPath)
			} else {
				monitor.LogMsg(monitor.LogInfo, "Received signal %v. Shutting down...", sig)
				cancel()
				return
			}
		}
	}()

	// Initialize and normalize safe sensors
	defaultTimeout := config.Conf.Timeout
	if defaultTimeout <= 0 {
		defaultTimeout = 5 // default to 5 seconds
	}
	if defaultTimeout < 100 {
		defaultTimeout = defaultTimeout * 1000
	}

	sensorsMu.Lock()
	safeSensors = make([]*monitor.SafeSensor, len(config.Sensors))
	for i := range config.Sensors {
		s := &config.Sensors[i]
		if s.Timeout <= 0 {
			s.Timeout = defaultTimeout
		} else if s.Timeout < 100 {
			s.Timeout = s.Timeout * 1000
		}
		safeSensors[i] = monitor.NewSafeSensor(s)
	}
	monitor.SetActiveSensors(safeSensors)
	sensorsMu.Unlock()

	// Channels for queuing work and metrics
	// Buffer sensor channels to avoid blocking the scheduler
	highPrioritySensorChan := make(chan *monitor.SafeSensor, 1000)
	lowPrioritySensorChan := make(chan *monitor.SafeSensor, 1000)
	metricChan := make(chan monitor.Metric, 1000)

	// Initialize outputs
	producer = monitor.NewMetricProducer(config)
	defer producer.Close()

	// Start IPC server
	socketPath := config.Conf.IpcSocketPath
	if socketPath == "" {
		socketPath = monitor.DefaultSocketPath()
	}
	startIPC(socketPath)

	// Spawn metric producer consumer
	go func() {
		for metric := range metricChan {
			producer.Produce(metric)
		}
	}()

	// Spawn worker pool
	threads := config.Conf.Threads
	if threads <= 0 {
		threads = 10
	}
	monitor.SetTotalWorkers(int64(threads))

	// Partition threads: allocate a portion to low priority sensors to prevent starvation of good ones
	lowPriorityThreads := threads / 2
	if lowPriorityThreads < 1 {
		lowPriorityThreads = 1
	}
	if threads > 1 && lowPriorityThreads >= threads {
		lowPriorityThreads = threads - 1
	}
	highPriorityThreads := threads - lowPriorityThreads

	var wg sync.WaitGroup

	// High priority workers only process the high priority queue, guaranteeing they are never blocked by slow/failed sensors
	for i := 0; i < highPriorityThreads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case ss, ok := <-highPrioritySensorChan:
					if !ok {
						return
					}
					monitor.ProcessSensor(ctx, ss, metricChan)
				case <-ctx.Done():
					return
				}
			}
		}(i)
	}

	// Low priority workers process the high priority queue with priority, and fall back to the low priority queue
	for i := 0; i < lowPriorityThreads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case ss, ok := <-highPrioritySensorChan:
					if !ok {
						return
					}
					monitor.ProcessSensor(ctx, ss, metricChan)
				case <-ctx.Done():
					return
				default:
					select {
					case ss, ok := <-highPrioritySensorChan:
						if !ok {
							return
						}
						monitor.ProcessSensor(ctx, ss, metricChan)
					case ss, ok := <-lowPrioritySensorChan:
						if !ok {
							return
						}
						monitor.ProcessSensor(ctx, ss, metricChan)
					case <-ctx.Done():
						return
					}
				}
			}
		}(i)
	}

	// Wait for workers to finish and close the metric channel
	go func() {
		wg.Wait()
		close(metricChan)
	}()

	// Scheduler Loop
	go func() {
		defer close(highPrioritySensorChan)
		defer close(lowPrioritySensorChan)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			sensorsMu.RLock()
			currentSensors := make([]*monitor.SafeSensor, len(safeSensors))
			copy(currentSensors, safeSensors)
			sleepMain := config.Conf.SleepMain
			sensorsMu.RUnlock()

			if len(currentSensors) == 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
				continue
			}

			staggerInterval := time.Duration(sleepMain) * time.Second / time.Duration(len(currentSensors))

			for _, ss := range currentSensors {
				select {
				case <-ctx.Done():
					return
				default:
					sensorsMu.RLock()
					stillActive := false
					for _, active := range safeSensors {
						if active == ss {
							stillActive = true
							break
						}
					}
					sensorsMu.RUnlock()

					if stillActive {
						if ss.IsLowPriority() {
							lowPrioritySensorChan <- ss
						} else {
							highPrioritySensorChan <- ss
						}
					}
				}

				if staggerInterval > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(staggerInterval):
					}
				}
			}
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()

	// Wait a moment for final metrics to be processed
	time.Sleep(500 * time.Millisecond)
	monitor.LogMsg(monitor.LogInfo, "redborder-monitor stopped successfully.")
}

func printHelp() {
	out := flag.CommandLine.Output()
	fmt.Fprintf(out, "Usage of redborder-monitor :\n\n")
	fmt.Fprintf(out, "Flags:\n")
	fmt.Fprintf(out, "  -c, --config <path>     Path to configuration file\n")
	fmt.Fprintf(out, "  -d, --debug <level>     Debug severity level (default 100)\n")
	fmt.Fprintf(out, "  -g, --daemon            Go Daemon mode (run in background)\n")
	fmt.Fprintf(out, "  -h, --help              Show help\n")
	fmt.Fprintf(out, "  -i, --info              Show available plugins and configuration JSON fields\n")
	fmt.Fprintf(out, "  -l, --list-mibs         List all loaded MIB modules and exit\n")
	fmt.Fprintf(out, "  -r, --reset             Reset daemon stats counters and exit\n")
	fmt.Fprintf(out, "  -s, --search-mibs <term> Search for loaded MIB objects by name (case-insensitive)\n")
	fmt.Fprintf(out, "  -t, --status            Show daemon status/stats and exit\n")
	fmt.Fprintf(out, "  -v, --version           Show version\n")
}

func reloadConfig(path string) {
	newConfig, err := monitor.LoadConfig(path)
	if err != nil {
		monitor.LogMsg(monitor.LogErr, "Failed to reload configuration: %v. Keeping current configuration.", err)
		return
	}

	// Normalize timeouts in the new config
	defaultTimeout := newConfig.Conf.Timeout
	if defaultTimeout <= 0 {
		defaultTimeout = 5
	}
	if defaultTimeout < 100 {
		defaultTimeout = defaultTimeout * 1000
	}
	for i := range newConfig.Sensors {
		s := &newConfig.Sensors[i]
		if s.Timeout <= 0 {
			s.Timeout = defaultTimeout
		} else if s.Timeout < 100 {
			s.Timeout = s.Timeout * 1000
		}
	}

	sensorsMu.Lock()
	defer sensorsMu.Unlock()

	// Update global config parameters
	config = newConfig
	if config.Conf.MaxSimultaneousQueries > 0 {
		monitor.MaxSimultaneousQueries = config.Conf.MaxSimultaneousQueries
	} else {
		monitor.MaxSimultaneousQueries = 10
	}
	if config.Conf.Debug > 0 {
		atomic.StoreInt32(&monitor.AtomicDebugLevel, int32(config.Conf.Debug))
	}

	// Update active producer configuration dynamically (e.g. topic, broker, endpoints)
	if producer != nil {
		producer.UpdateConfig(newConfig)
	}

	// Update IPC listener if socket path changed
	newSocketPath := newConfig.Conf.IpcSocketPath
	if newSocketPath == "" {
		newSocketPath = monitor.DefaultSocketPath()
	}
	oldSocketPath := config.Conf.IpcSocketPath
	if oldSocketPath == "" {
		oldSocketPath = monitor.DefaultSocketPath()
	}

	if newSocketPath != oldSocketPath {
		monitor.LogMsg(monitor.LogInfo, "IPC socket path changed from %s to %s. Restarting IPC server...", oldSocketPath, newSocketPath)
		startIPC(newSocketPath)
	}

	// Build a map of existing sensors by name for lookup
	existingMap := make(map[string]*monitor.SafeSensor)
	for _, ss := range safeSensors {
		existingMap[ss.Sensor.SensorName] = ss
	}

	var updatedSensors []*monitor.SafeSensor
	for i := range newConfig.Sensors {
		s := &newConfig.Sensors[i]
		if ss, exists := existingMap[s.SensorName]; exists {
			// Acquire the sensor mutex to safely update the definition and enrichment
			ss.Mutex.Lock()
			ss.Sensor = s

			// Rebuild and update sensor-level enrichment
			enrich := make(map[string]interface{})
			for k, v := range s.Enrichment {
				enrich[k] = v
			}
			enrich["sensor_name"] = s.SensorName
			if s.SensorID > 0 {
				enrich["sensor_id"] = s.SensorID
			}
			ss.Enrichment = enrich

			ss.Mutex.Unlock()
			updatedSensors = append(updatedSensors, ss)
			monitor.LogMsg(monitor.LogInfo, "Updated configuration for existing sensor '%s'", s.SensorName)
		} else {
			// Create a new SafeSensor for the new sensor
			newSS := monitor.NewSafeSensor(s)
			updatedSensors = append(updatedSensors, newSS)
			monitor.LogMsg(monitor.LogInfo, "Added new sensor '%s'", s.SensorName)
		}
	}

	safeSensors = updatedSensors
	monitor.SetActiveSensors(safeSensors)
	monitor.LogMsg(monitor.LogInfo, "Configuration reloaded successfully. Total active sensors: %d", len(safeSensors))
}

func startIPC(socketPath string) {
	ipcListenerMu.Lock()
	defer ipcListenerMu.Unlock()

	if ipcListener != nil {
		_ = ipcListener.Close()
		ipcListener = nil
	}

	var err error
	ipcListener, err = monitor.StartIPCServer(socketPath)
	if err != nil {
		monitor.LogMsg(monitor.LogErr, "Failed to start IPC status server on %s: %v", socketPath, err)
	} else {
		monitor.LogMsg(monitor.LogInfo, "IPC status server listening on %s", socketPath)
	}
}

func queryStatus(configPath string) {
	socketPath := monitor.DefaultSocketPath()
	if configPath != "" {
		if cfg, err := monitor.LoadConfig(configPath); err == nil && cfg.Conf.IpcSocketPath != "" {
			socketPath = cfg.Conf.IpcSocketPath
		}
	}

	network, address := monitor.ParseIPCAddress(socketPath)

	conn, err := net.Dial(network, address)
	if err != nil {
		fmt.Printf("Error: could not connect to daemon at %s (%s): %v\n", socketPath, network, err)
		os.Exit(1)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("stats"))
	if err != nil {
		fmt.Printf("Error writing to socket: %v\n", err)
		os.Exit(1)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, conn)
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(buf.String())
}

func sendResetCommand(configPath string) {
	socketPath := monitor.DefaultSocketPath()
	if configPath != "" {
		if cfg, err := monitor.LoadConfig(configPath); err == nil && cfg.Conf.IpcSocketPath != "" {
			socketPath = cfg.Conf.IpcSocketPath
		}
	}

	network, address := monitor.ParseIPCAddress(socketPath)

	conn, err := net.Dial(network, address)
	if err != nil {
		fmt.Printf("Error: could not connect to daemon at %s (%s): %v\n", socketPath, network, err)
		os.Exit(1)
	}
	defer conn.Close()

	_, err = conn.Write([]byte("reset"))
	if err != nil {
		fmt.Printf("Error writing to socket: %v\n", err)
		os.Exit(1)
	}

	var buf bytes.Buffer
	_, err = io.Copy(&buf, conn)
	if err != nil {
		fmt.Printf("Error reading response: %v\n", err)
		os.Exit(1)
	}

	fmt.Print(buf.String())
}

func searchMIBObjects(term string) {
	// First initialize MIBs based on config if configPath is provided
	var mibDirs []string
	if configPath != "" {
		if cfg, err := monitor.LoadConfig(configPath); err == nil {
			mibDirs = cfg.Conf.MibDirs
		} else {
			fmt.Printf("Warning: Failed to load config file: %v. Using system default MIB paths.\n", err)
		}
	}

	// Initialize the MIB engine
	_ = monitor.InitMIBs(mibDirs)

	monitor.MibEngineMu.RLock()
	engine := monitor.MibEngine
	monitor.MibEngineMu.RUnlock()

	if engine == nil {
		fmt.Println("Error: MIB engine is not initialized or failed to load standard system MIBs.")
		return
	}

	termLower := strings.ToLower(term)
	termWithoutDot := termLower
	if strings.HasPrefix(termWithoutDot, ".") {
		termWithoutDot = termWithoutDot[1:]
	}
	objects := engine.Objects()

	var matchedCount int
	fmt.Printf("Searching for MIB objects matching %q...\n", term)
	fmt.Println(strings.Repeat("-", 125))
	fmt.Printf("%-55s %-40s %-12s %-12s\n", "Symbol (Module::Object)", "Numeric OID", "Access", "Status")
	fmt.Println(strings.Repeat("-", 125))

	for _, obj := range objects {
		fullName := obj.Name()
		if obj.Module() != nil {
			fullName = obj.Module().Name() + "::" + obj.Name()
		}

		if strings.Contains(strings.ToLower(obj.Name()), termLower) ||
			strings.Contains(strings.ToLower(fullName), termLower) ||
			strings.Contains(strings.ToLower(obj.OID().String()), termWithoutDot) {

			matchedCount++

			// Format OID
			oidStr := obj.OID().String()
			if !strings.HasPrefix(oidStr, ".") {
				oidStr = "." + oidStr
			}

			fmt.Printf("%-55s %-40s %-12s %-12s\n",
				truncateString(fullName, 53),
				truncateString(oidStr, 38),
				obj.Access().String(),
				obj.Status().String(),
			)
		}
	}

	fmt.Println(strings.Repeat("-", 125))
	fmt.Printf("Found %d matching MIB objects.\n", matchedCount)
}

func truncateString(s string, maxLen int) string {
	if len(s) > maxLen {
		return s[:maxLen-3] + "..."
	}
	return s
}

func listMIBModules() {
	var mibDirs []string
	if configPath != "" {
		if cfg, err := monitor.LoadConfig(configPath); err == nil {
			mibDirs = cfg.Conf.MibDirs
		}
	}

	_ = monitor.InitMIBs(mibDirs)

	monitor.MibEngineMu.RLock()
	engine := monitor.MibEngine
	monitor.MibEngineMu.RUnlock()

	if engine == nil {
		fmt.Println("Error: MIB engine is not initialized.")
		return
	}

	modules := engine.Modules()
	fmt.Printf("Loaded MIB Modules (%d total):\n", len(modules))
	fmt.Println(strings.Repeat("-", 90))
	fmt.Printf("%-35s %-10s %-40s\n", "Module Name", "Objects", "Source File")
	fmt.Println(strings.Repeat("-", 90))

	for _, mod := range modules {
		objCount := len(mod.Objects())
		srcPath := mod.SourcePath()
		if srcPath == "" {
			srcPath = "(built-in/discovered)"
		}
		fmt.Printf("%-35s %-10d %-40s\n",
			mod.Name(),
			objCount,
			truncateString(srcPath, 38),
		)
	}
	fmt.Println(strings.Repeat("-", 90))
}
