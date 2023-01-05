class CircularQueue {
  long capacity;
  long *container;
  long head, tail;
  long size;

public:
  CircularQueue(long _capacity)
      : capacity(_capacity), head(0), tail(0), size(0) {
    if (capacity < 1) {
      std::cout << "Capacity of the queue cannot be less than 1\n";
    }
    container = new long[capacity];
  }

  ~CircularQueue() { delete[] container; }

  bool isFull() { return size == capacity; }

  bool isEmpty() { return size == 0; }

  long itemCount() { return size; }
  
  long getCapacity() { return capacity; }

  // Returns True if queue is successfully enqueued.
  // Returns False if queue is full and item cannot be enqueued.
  bool enqueue(long value) {
    if (isFull()) {
      return false;
    }
    container[tail] = value;
    tail = (tail + 1) % capacity;
    size++;
    return true;
  }

  // Returns True if queue is not empty and the dequeued item is copied to value
  // Returns False if queue is empty
  bool dequeue(long *value) {
    if (isEmpty()) {
      return false;
    }
    *value = container[head];
    head = (head + 1) % capacity;
    size--;
    return true;
  }
};