package monitor

import (
	"context"
	"sync"
	"testing"
)

func TestParseRawOutput(t *testing.T) {
	// 1. Scalar output
	mScalar := &Monitor{
		Name: "test_scalar",
	}
	stateScalar := parseRawOutput("12.5", mScalar)
	if stateScalar.Scalar == nil {
		t.Fatal("Scalar should not be nil")
	}
	if stateScalar.Scalar.Value != 12.5 {
		t.Errorf("Expected scalar value 12.5, got %f", stateScalar.Scalar.Value)
	}

	// 2. Vector output with split and split_op
	mVector := &Monitor{
		Name:    "test_vector",
		Split:   ";",
		SplitOp: "sum",
	}
	stateVector := parseRawOutput("1.0; 2.0; 3.0", mVector)
	if len(stateVector.Children) != 3 {
		t.Fatalf("Expected 3 children, got %d", len(stateVector.Children))
	}
	if stateVector.Children[0].Value != 1.0 || stateVector.Children[1].Value != 2.0 || stateVector.Children[2].Value != 3.0 {
		t.Errorf("Incorrect child values")
	}
	if stateVector.SplitOp == nil {
		t.Fatal("SplitOp should not be nil")
	}
	if stateVector.SplitOp.Value != 6.0 {
		t.Errorf("Expected SplitOp aggregate 6.0, got %f", stateVector.SplitOp.Value)
	}

	// 3. Vector with timestamps
	tg := 1
	mVectorTime := &Monitor{
		Name:           "test_vector_time",
		Split:          ";",
		SplitOp:        "mean",
		TimestampGiven: &tg,
	}
	stateVectorTime := parseRawOutput("10:20; 30:40", mVectorTime)
	if len(stateVectorTime.Children) != 2 {
		t.Fatalf("Expected 2 children, got %d", len(stateVectorTime.Children))
	}
	if stateVectorTime.Children[0].Timestamp != 10 || stateVectorTime.Children[0].Value != 20 {
		t.Errorf("Incorrect first child timestamp/value: %v", stateVectorTime.Children[0])
	}
	if stateVectorTime.Children[1].Timestamp != 30 || stateVectorTime.Children[1].Value != 40 {
		t.Errorf("Incorrect second child timestamp/value: %v", stateVectorTime.Children[1])
	}
	if stateVectorTime.SplitOp == nil {
		t.Fatal("SplitOp should not be nil")
	}
	if stateVectorTime.SplitOp.Value != 30.0 {
		t.Errorf("Expected SplitOp aggregate mean 30.0, got %f", stateVectorTime.SplitOp.Value)
	}
}

func TestOutputMetricsCaching(t *testing.T) {
	tg := 1
	m := &Monitor{
		Name:           "load_5",
		TimestampGiven: &tg,
	}

	enrich := map[string]interface{}{"sensor_name": "test-sensor"}
	metricChan := make(chan Metric, 10)

	// First run: cache is empty, should send
	state1 := &MonitorState{
		Scalar: &MonitorValue{Timestamp: 10, Value: 0.15},
	}
	outputMetrics(m, state1, nil, enrich, metricChan)
	if len(metricChan) != 1 {
		t.Errorf("Expected 1 metric sent, got %d", len(metricChan))
	}
	<-metricChan

	// Second run: timestamp is same, value is same, should NOT send
	outputMetrics(m, state1, state1, enrich, metricChan)
	if len(metricChan) != 0 {
		t.Errorf("Expected 0 metrics sent (caching active), got %d", len(metricChan))
	}

	// Third run: timestamp increases, should send
	state3 := &MonitorState{
		Scalar: &MonitorValue{Timestamp: 20, Value: 0.15},
	}
	outputMetrics(m, state3, state1, enrich, metricChan)
	if len(metricChan) != 1 {
		t.Errorf("Expected 1 metric sent (timestamp increased), got %d", len(metricChan))
	}
	<-metricChan

	// Fourth run: value changes, should send
	state4 := &MonitorState{
		Scalar: &MonitorValue{Timestamp: 20, Value: 0.25},
	}
	outputMetrics(m, state4, state3, enrich, metricChan)
	if len(metricChan) != 1 {
		t.Errorf("Expected 1 metric sent (value changed), got %d", len(metricChan))
	}
	<-metricChan
}

func TestResolveCROSSGroupOp(t *testing.T) {
	// Verify cross-group name lookup
	currentVals := make(map[string]*MonitorState)
	
	// Group 6 monitor "wire_mbits_per_sec_realtime"
	currentVals["wire_mbits_per_sec_realtime_gid_6"] = &MonitorState{
		Scalar: &MonitorValue{Timestamp: 100, Value: 2.5},
	}

	m := &Monitor{
		Name:    "wire_bits_per_sec_realtime",
		Op:      "1000000*wire_mbits_per_sec_realtime_gid_6",
		GroupID: float64(6),
	}

	s := &Sensor{
		SensorName: "test-sensor",
	}
	ss := NewSafeSensor(s)

	resState := processOpMonitor(context.Background(), ss, m, currentVals)
	if resState == nil || resState.Scalar == nil {
		t.Fatal("OP resolution failed")
	}

	expected := 2500000.0
	if resState.Scalar.Value != expected {
		t.Errorf("Expected evaluated value %f, got %f", expected, resState.Scalar.Value)
	}
}

func TestProcessSensorAsync(t *testing.T) {
	s := &Sensor{
		SensorID:    1,
		SensorName:  "test-sensor",
		SensorIP:    "127.0.0.1",
		SnmpVersion: "2c",
		Community:   "public",
		Timeout:     5000,
		Monitors: []Monitor{
			{
				Name:   "sys_val",
				System: "echo 42",
			},
			{
				Name: "op_val",
				Op:   "2 * sys_val",
			},
		},
	}

	ss := NewSafeSensor(s)
	metricChan := make(chan Metric, 10)
	ctx := context.Background()

	ProcessSensor(ctx, ss, metricChan)
	close(metricChan)

	var metrics []Metric
	for m := range metricChan {
		metrics = append(metrics, m)
	}

	if len(metrics) != 2 {
		t.Fatalf("Expected 2 metrics, got %d", len(metrics))
	}

	// First metric should be sys_val
	if metrics[0].Monitor != "sys_val" {
		t.Errorf("Expected first metric to be sys_val, got %s", metrics[0].Monitor)
	}
	if valStr, ok := metrics[0].Value.(string); !ok || valStr != "42.000000" {
		t.Errorf("Expected sys_val value '42.000000', got %v", metrics[0].Value)
	}

	// Second metric should be op_val
	if metrics[1].Monitor != "op_val" {
		t.Errorf("Expected second metric to be op_val, got %s", metrics[1].Monitor)
	}
	if valStr, ok := metrics[1].Value.(string); !ok || valStr != "84.000000" {
		t.Errorf("Expected op_val value '84.000000', got %v", metrics[1].Value)
	}
}

func TestSafeSensorPriority(t *testing.T) {
	// 1. Fast sensor (High Priority)
	sFast := &Sensor{
		SensorID:   1,
		SensorName: "fast-sensor",
		Timeout:    2000,
		Monitors: []Monitor{
			{
				Name:   "fast_cmd",
				System: "echo fast",
			},
		},
	}
	ssFast := NewSafeSensor(sFast)
	metricChan := make(chan Metric, 10)
	ProcessSensor(context.Background(), ssFast, metricChan)
	if ssFast.IsLowPriority() {
		t.Errorf("Expected fast-sensor to not be low priority")
	}

	// 2. Slow sensor (Low Priority)
	sSlow := &Sensor{
		SensorID:   2,
		SensorName: "slow-sensor",
		Timeout:    1000,
		Monitors: []Monitor{
			{
				Name:   "slow_cmd",
				System: "sleep 1.2",
			},
		},
	}
	ssSlow := NewSafeSensor(sSlow)
	ProcessSensor(context.Background(), ssSlow, metricChan)
	if !ssSlow.IsLowPriority() {
		t.Errorf("Expected slow-sensor to be marked as low priority")
	}
}

func TestProcessSensor_SkipConcurrent(t *testing.T) {
	s := &Sensor{
		SensorID:   1,
		SensorName: "lock-sensor",
		Timeout:    1000,
		Monitors: []Monitor{
			{
				Name:   "sys_cmd",
				System: "sleep 0.5",
			},
		},
	}
	ss := NewSafeSensor(s)

	metricChan := make(chan Metric, 10)
	ctx := context.Background()

	ss.Mutex.Lock()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ProcessSensor(ctx, ss, metricChan)
	}()

	wg.Wait()
	ss.Mutex.Unlock()

	close(metricChan)
	if len(metricChan) != 0 {
		t.Errorf("expected no metrics to be processed because sensor was locked, got %d", len(metricChan))
	}
}

func TestProcessSensor_TransitionLowPriorityOnError(t *testing.T) {
	s := &Sensor{
		SensorID:   2,
		SensorName: "fail-sensor",
		Timeout:    2000,
		Monitors: []Monitor{
			{
				Name:   "fail_cmd",
				System: "exit 1",
			},
		},
	}
	ss := NewSafeSensor(s)
	metricChan := make(chan Metric, 10)

	if ss.IsLowPriority() {
		t.Fatal("initially sensor should not be low priority")
	}

	ProcessSensor(context.Background(), ss, metricChan)

	if !ss.IsLowPriority() {
		t.Errorf("expected sensor to transition to low priority because command failed")
	}
}

func TestProcessOpMonitor_MissingDependency(t *testing.T) {
	m := &Monitor{
		Name: "op_val",
		Op:   "a + b",
	}

	currentVals := make(map[string]*MonitorState)
	currentVals["a"] = &MonitorState{
		Scalar: &MonitorValue{Value: 10},
	}

	s := &Sensor{SensorName: "test"}
	ss := NewSafeSensor(s)

	res := processOpMonitor(context.Background(), ss, m, currentVals)
	if res != nil {
		t.Errorf("expected nil state because dependency 'b' is missing, got %v", res)
	}
}

func TestProcessOpMonitor_VectorLengthMismatch(t *testing.T) {
	m := &Monitor{
		Name: "op_val",
		Op:   "a + b",
	}

	currentVals := make(map[string]*MonitorState)
	currentVals["a"] = &MonitorState{
		Children: []*MonitorValue{
			{Value: 1},
			{Value: 2},
		},
	}
	currentVals["b"] = &MonitorState{
		Children: []*MonitorValue{
			{Value: 10},
			{Value: 20},
			{Value: 30},
		},
	}

	s := &Sensor{SensorName: "test"}
	ss := NewSafeSensor(s)

	res := processOpMonitor(context.Background(), ss, m, currentVals)
	if res != nil {
		t.Errorf("expected nil state due to vector length mismatch, got %v", res)
	}
}
