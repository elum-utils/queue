package queue

import (
	"testing"
	"time"
)

func setupQueue(t *testing.T, config Config) *Queue {
	t.Helper()
	q, err := New(config)
	if err != nil {
		t.Fatalf("failed to initialize queue: %v", err)
	}
	return q
}

func TestQueue_AddAndGet(t *testing.T) {
	queue := setupQueue(t, Config{})
	defer queue.Close()

	// Добавляем элементы в очередь
	data1 := []byte("test data 1")
	data2 := []byte("test data 2")

	if err := queue.Add(data1); err != nil {
		t.Fatalf("failed to add item to queue: %v", err)
	}
	if err := queue.Add(data2); err != nil {
		t.Fatalf("failed to add item to queue: %v", err)
	}

	// Получаем элементы из очереди
	items, err := queue.Get(2)
	if err != nil {
		t.Fatalf("failed to get items from queue: %v", err)
	}

	// Проверяем количество полученных элементов
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}

	// Проверяем содержимое элементы
	if string(items[0].Data) != "test data 1" || string(items[1].Data) != "test data 2" {
		t.Fatalf("unexpected item data")
	}
}

func TestQueue_Delete(t *testing.T) {
	queue := setupQueue(t, Config{})
	defer queue.Close()

	// Добавляем элемент в очередь
	data := []byte("test data")
	if err := queue.Add(data); err != nil {
		t.Fatalf("failed to add item to queue: %v", err)
	}

	// Получаем элемент из очереди
	items, err := queue.Get(1)
	if err != nil {
		t.Fatalf("failed to get item from queue: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}

	// Удаляем элемент из очереди
	if err := queue.Delete(items[0].ID); err != nil {
		t.Fatalf("failed to delete item from queue: %v", err)
	}

	// Проверяем, что очередь теперь пуста
	items, err = queue.Get(1)
	if err != nil {
		t.Fatalf("failed to get item from queue: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected queue to be empty, got %d", len(items))
	}
}

func TestIndependentQueues(t *testing.T) {
	// Создаем две независимые in-memory базы
	q1 := setupQueue(t, Config{LocalFile: "file:memdb1?mode=memory&cache=shared"})
	defer q1.Close()
	q2 := setupQueue(t, Config{LocalFile: "file:memdb2?mode=memory&cache=shared"})
	defer q2.Close()

	// Добавляем элементы в каждую очередь
	data1 := []byte("queue1 data")
	data2 := []byte("queue2 data")

	if err := q1.Add(data1); err != nil {
		t.Fatalf("failed to add item to queue1: %v", err)
	}

	if err := q2.Add(data2); err != nil {
		t.Fatalf("failed to add item to queue2: %v", err)
	}

	// Проверяем, что данные в q1 не повлияли на q2 и наоборот
	items1, err := q1.Get(1)
	if err != nil {
		t.Fatalf("failed to get items from queue1: %v", err)
	}
	if len(items1) != 1 || string(items1[0].Data) != "queue1 data" {
		t.Fatalf("queue1 has unexpected items: %+v", items1)
	}

	items2, err := q2.Get(1)
	if err != nil {
		t.Fatalf("failed to get items from queue2: %v", err)
	}
	if len(items2) != 1 || string(items2[0].Data) != "queue2 data" {
		t.Fatalf("queue2 has unexpected items: %+v", items2)
	}
}

func TestCallbackInvocation(t *testing.T) {
	queue := setupQueue(t, Config{})

	// Add items to the queue
	err := queue.Add([]byte("test data 1"))
	if err != nil {
		t.Fatalf("failed to add item to queue: %v", err)
	}

	err = queue.Add([]byte("test data 2"))
	if err != nil {
		t.Fatalf("failed to add item to queue: %v", err)
	}

	// Channel to capture callback invocations
	callbackInvocations := make(chan Item, 2)

	// Set the callback function
	queue.Listener(func(item Item, broken func()) {
		callbackInvocations <- item
	})

	// Give some time for the processing goroutine to execute
	time.Sleep(5 * time.Second) // Adjust the sleep time as necessary for your environment

	close(callbackInvocations)

	// Verify that each item was processed
	processedItems := []string{}
	for item := range callbackInvocations {
		processedItems = append(processedItems, string(item.Data))
	}

	if len(processedItems) != 2 {
		t.Fatalf("expected 2 callbacks, got %d", len(processedItems))
	}

	expectedItems := []string{"test data 1", "test data 2"}
	for i, item := range processedItems {
		if item != expectedItems[i] {
			t.Errorf("expected %s, got %s", expectedItems[i], item)
		}
	}
}
