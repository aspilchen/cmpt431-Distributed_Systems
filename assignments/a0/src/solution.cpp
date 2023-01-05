#include "solution.h"

void producerFunction(void *_arg) {
  SharedMemory* mem = static_cast<SharedMemory*>(_arg);
  long item = 0;
  mem->items_processed = 0;
  mem->value = 0;

  while(mem->items_processed < mem->n_items) {
    pthread_mutex_lock(mem->buffer_mutex);
    if(mem->production_buffer->enqueue(item)) {
      if(mem->production_buffer->itemCount() == 1) {
        pthread_cond_broadcast(mem->consumer_condition);
      }
      mem->items_processed++;
      mem->value += item;
      item++;
    }
    else {
      pthread_cond_wait(mem->producer_condition, mem->buffer_mutex);
    }
    pthread_mutex_unlock(mem->buffer_mutex);
  }

  pthread_mutex_lock(mem->active_count_mutex);
  *(mem->active_producer_count) -= 1;
  if(*(mem->active_producer_count) == 0) {
    pthread_mutex_unlock(mem->active_count_mutex);
    while(*(mem->active_consumer_count) > 0) {
      pthread_cond_broadcast(mem->consumer_condition);
    }
  }
  else{
    pthread_mutex_unlock(mem->active_count_mutex);
  }
}

void consumerFunction(void *_arg) {
  SharedMemory* mem = static_cast<SharedMemory*>(_arg);
  long item = 0;
  mem->items_processed = 0;
  mem->value = 0;
  long foo = 1000;

  while(true) {
    pthread_mutex_lock(mem->buffer_mutex);
    if(mem->production_buffer->dequeue(&item)) {
      if(mem->production_buffer->itemCount() == mem->production_buffer->getCapacity() - 1) {
        pthread_cond_broadcast(mem->producer_condition);
      }
      mem->items_processed++;
      mem->value += item;
    }
    else {
      pthread_mutex_lock(mem->active_count_mutex);
      if(*(mem->active_producer_count) > 0) {
        pthread_mutex_unlock(mem->active_count_mutex);
        pthread_cond_wait(mem->consumer_condition, mem->buffer_mutex);
      }
      else {
        pthread_mutex_unlock(mem->buffer_mutex);
        pthread_mutex_unlock(mem->active_count_mutex);
        break;
      }
    }
    pthread_mutex_unlock(mem->buffer_mutex);
  }

  pthread_mutex_lock(mem->active_count_mutex);
  *(mem->active_consumer_count) -= 1;
  pthread_mutex_unlock(mem->active_count_mutex);
}

void* producerTask(void* _arg) {
  SharedMemory* mem = static_cast<SharedMemory*>(_arg);
  timer t;
  t.start();
  producerFunction(_arg);
  mem->time_taken = t.stop();
  return NULL;
}

void* consumerTask(void* _arg) {
  SharedMemory* mem = static_cast<SharedMemory*>(_arg);
  timer t;
  t.start();
  consumerFunction(_arg);
  mem->time_taken = t.stop();
  return NULL;
}

ProducerConsumerProblem::ProducerConsumerProblem(long _n_items,
                                                 int _n_producers,
                                                 int _n_consumers,
                                                 long _queue_size)
    : n_items(_n_items), n_producers(_n_producers), n_consumers(_n_consumers),
      production_buffer(_queue_size) {
  std::cout << "Constructor\n";
  std::cout << "Number of items: " << n_items << "\n";
  std::cout << "Number of producers: " << n_producers << "\n";
  std::cout << "Number of consumers: " << n_consumers << "\n";
  std::cout << "Queue size: " << _queue_size << "\n";

  producer_threads = new pthread_t[n_producers];
  consumer_threads = new pthread_t[n_consumers];

  producer_memory = new SharedMemory[n_producers];
  consumer_memory = new SharedMemory[n_consumers];

  // Initialize all mutex and conditional variables here.
  pthread_mutex_init(&buffer_mutex, NULL);
  pthread_mutex_init(&active_count_mutex, NULL);
  pthread_cond_init(&producer_condition, NULL);
  pthread_cond_init(&consumer_condition, NULL);
}

ProducerConsumerProblem::~ProducerConsumerProblem() {
  std::cout << "Destructor\n";
  delete[] producer_threads;
  delete[] consumer_threads;
  // Destroy all mutex and conditional variables here.
  pthread_mutex_destroy(&buffer_mutex);
  pthread_mutex_destroy(&active_count_mutex);
  pthread_cond_destroy(&producer_condition);
  pthread_cond_destroy(&consumer_condition);
  delete producer_memory;
  delete consumer_memory;
}

void ProducerConsumerProblem::initSharedMem(SharedMemory* mem) {
  mem->items_processed = 0;
  mem->value = 0;
  mem->n_items = n_items;
  mem->time_taken = 0;
  mem->production_buffer = &production_buffer;
  mem->buffer_mutex = &buffer_mutex;
  mem->active_count_mutex = &active_count_mutex;
  mem->producer_condition = &producer_condition;
  mem->consumer_condition = &consumer_condition;
  mem->active_producer_count = &active_producer_count;
  mem->active_consumer_count = &active_consumer_count;
}

void ProducerConsumerProblem::startProducers() {
  std::cout << "Starting Producers\n";
  active_producer_count = n_producers;
  for(int i = 0; i < n_producers; i++) {
    initSharedMem(&producer_memory[i]);
    pthread_create(&producer_threads[i], NULL, producerTask, static_cast<void*>(&producer_memory[i]));
  }
}

void ProducerConsumerProblem::startConsumers() {
  std::cout << "Starting Consumers\n";
  active_consumer_count = n_consumers;
  for(int i = 0; i < n_consumers; i++) {
    initSharedMem(&consumer_memory[i]);
    pthread_create(&consumer_threads[i], NULL, consumerTask, static_cast<void*>(&consumer_memory[i]));
  }
  // Create consumer threads C1, C2, C3,.. using pthread_create.
}

void ProducerConsumerProblem::joinProducers() {
  std::cout << "Joining Producers\n";
  for(int i = 0; i < n_producers; i++) {
    pthread_join(producer_threads[i], NULL);
  }
}

void ProducerConsumerProblem::joinConsumers() {
  std::cout << "Joining Consumers\n";
  for(int i = 0; i < n_consumers; i++) {
    pthread_join(consumer_threads[i], NULL);
  }
}

void ProducerConsumerProblem::printStats() {
  std::cout << "Producer stats\n";
  std::cout << "producer_id, items_produced, value_produced, time_taken \n";
  for(int i = 0; i < n_producers; i++) {
    std::cout << i << ", ";
    std::cout << producer_memory[i].items_processed << ", ";
    std::cout << producer_memory[i].value << ", ";
    std::cout << producer_memory[i].time_taken << "\n";
  }
  long total_produced = 0;
  long total_value_produced = 0;
  for (int i = 0; i < n_producers; i++) {
    total_produced += producer_memory[i].items_processed;
    total_value_produced += producer_memory[i].value;
  }
  std::cout << "Total produced = " << total_produced << "\n";
  std::cout << "Total value produced = " << total_value_produced << "\n";
  std::cout << "Consumer stats\n";
  std::cout << "consumer_id, items_consumed, value_consumed, time_taken \n";
  for(int i = 0; i < n_consumers; i++) {
    std::cout << i << ", ";
    std::cout << consumer_memory[i].items_processed << ", ";
    std::cout << consumer_memory[i].value << ", ";
    std::cout << consumer_memory[i].time_taken << "\n";
  }
  long total_consumed = 0;
  long total_value_consumed = 0;
  for (int i = 0; i < n_consumers; i++) {
    total_consumed += consumer_memory[i].items_processed;
    total_value_consumed += consumer_memory[i].value;
    // ---
    //
    // ---
  }
  std::cout << "Total consumed = " << total_consumed << "\n";
  std::cout << "Total value consumed = " << total_value_consumed << "\n";
}