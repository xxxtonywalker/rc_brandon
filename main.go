package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"
)

// --- 1. 模型定义 ---

type NotificationRequest struct {
	TargetID       string                 `json:"target_id"`
	Event          string                 `json:"event"`
	IdempotencyKey string                 `json:"idempotency_key"`
	Payload        map[string]interface{} `json:"payload"`
}

type TargetConfig struct {
	URL      string
	Headers  map[string]string
	Template func(req NotificationRequest) []byte
}

type Task struct {
	Request    NotificationRequest
	RetryCount int
	NextRun    time.Time
}

// --- 2. 模拟延迟消息队列 (Mock MQ) ---

type MockMQ struct {
	queue chan Task
	wg    *sync.WaitGroup
}

func NewMockMQ(buffer int, wg *sync.WaitGroup) *MockMQ {
	return &MockMQ{
		queue: make(chan Task, buffer),
		wg:    wg,
	}
}

func (m *MockMQ) Push(task Task) {
	m.queue <- task
}

func (m *MockMQ) PushDelayed(task Task, delay time.Duration) {
	fmt.Printf("[MQ] 任务 %s 将在 %v 后进行第 %d 次重试...\n", task.Request.IdempotencyKey, delay, task.RetryCount)
	time.AfterFunc(delay, func() {
		m.queue <- task
	})
}

func (m *MockMQ) Consume() <-chan Task {
	return m.queue
}

// --- 3. 核心投递引擎 (Worker) ---

type DeliveryEngine struct {
	configs map[string]TargetConfig
	mq      *MockMQ
	client  *http.Client
}

func NewDeliveryEngine(mq *MockMQ) *DeliveryEngine {
	configs := make(map[string]TargetConfig)

	configs["AD_SYSTEM"] = TargetConfig{
		Headers: map[string]string{"Content-Type": "application/json", "X-Ad-Key": "secret-123"},
		Template: func(req NotificationRequest) []byte {
			data, _ := json.Marshal(map[string]interface{}{
				"ad_event": req.Event,
				"user":     req.Payload["user_id"],
			})
			return data
		},
	}

	configs["CRM_SYSTEM"] = TargetConfig{
		Headers: map[string]string{"Authorization": "Bearer token-abc", "Content-Type": "application/x-www-form-urlencoded"},
		Template: func(req NotificationRequest) []byte {
			return []byte(fmt.Sprintf("event=%s&user=%v", req.Event, req.Payload["email"]))
		},
	}

	return &DeliveryEngine{
		configs: configs,
		mq:      mq,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (e *DeliveryEngine) Start() {
	go func() {
		for task := range e.mq.Consume() {
			go e.deliver(task)
		}
	}()
}

func (e *DeliveryEngine) deliver(task Task) {
	config, ok := e.configs[task.Request.TargetID]
	if !ok {
		fmt.Printf("[Worker] 错误：未知的目标系统 %s\n", task.Request.TargetID)
		e.mq.wg.Done()
		return
	}

	body := config.Template(task.Request)
	req, _ := http.NewRequest("POST", config.URL, bytes.NewBuffer(body))
	for k, v := range config.Headers {
		req.Header.Set(k, v)
	}

	resp, err := e.client.Do(req)

	// 判定失败：网络错误或非2xx状态码
	if err != nil || (resp != nil && (resp.StatusCode < 200 || resp.StatusCode >= 300)) {
		status := "Network Error"
		if resp != nil {
			status = resp.Status
			resp.Body.Close() // 及时关闭失败的响应体
		}
		fmt.Printf("[Worker] 投递失败 (%s), ID: %s, Retry: %d\n", status, task.Request.IdempotencyKey, task.RetryCount)

		if task.RetryCount < 3 {
			task.RetryCount++
			delay := time.Duration(math.Pow(2, float64(task.RetryCount-1))) * time.Second
			e.mq.PushDelayed(task, delay)
		} else {
			fmt.Printf("[Worker] !!! 任务彻底失败 (DLQ): %s\n", task.Request.IdempotencyKey)
			e.mq.wg.Done()
		}
		return
	}

	// 成功处理
	if resp != nil {
		defer resp.Body.Close()
	}
	fmt.Printf("[Worker] 投递成功! Target: %s, ID: %s, Status: %s\n", task.Request.TargetID, task.Request.IdempotencyKey, resp.Status)
	e.mq.wg.Done()
}

// --- 4. 主程序运行 ---

func main() {
	var wg sync.WaitGroup
	mq := NewMockMQ(100, &wg)
	engine := NewDeliveryEngine(mq)

	// 修正点：这里的参数必须是 *http.Request
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 模拟 1/3 的概率返回 500 错误
		if time.Now().UnixNano()%3 == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer mockServer.Close()

	for k, v := range engine.configs {
		v.URL = mockServer.URL
		engine.configs[k] = v
	}

	engine.Start()

	incomingRequests := []NotificationRequest{
		{
			TargetID:       "AD_SYSTEM",
			Event:          "USER_REG",
			IdempotencyKey: "req_001",
			Payload:        map[string]interface{}{"user_id": "u123"},
		},
		{
			TargetID:       "CRM_SYSTEM",
			Event:          "SUB_SUCCESS",
			IdempotencyKey: "req_002",
			Payload:        map[string]interface{}{"email": "test@example.com"},
		},
	}

	fmt.Println(">>> 接入层开始接收并推送任务...")
	for _, req := range incomingRequests {
		wg.Add(1)
		mq.Push(Task{Request: req, RetryCount: 0})
	}

	wg.Wait()
	fmt.Println(">>> 演示结束。")
}
