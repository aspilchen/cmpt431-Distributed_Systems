#include "core/circular_queue.h"
#include "core/utils.h"
#include <pthread.h>
#include <stdlib.h>
#include <algorithm>

struct SharedMemory {
  long items_processed;
  long value;
  long n_items;
  double time_taken;
  CircularQueue* production_buffer;
  pthread_mutex_t* buffer_mutex;
  pthread_mutex_t* active_count_mutex;
  pthread_cond_t* producer_condition;
  pthread_cond_t* consumer_condition;
  int* active_producer_count;
  int* active_consumer_count;
};

class ProducerConsumerProblem {
  long n_items;
  int n_producers;
  int n_consumers;
  CircularQueue production_buffer;

  pthread_mutex_t buffer_mutex;
  pthread_mutex_t active_count_mutex;
  pthread_cond_t producer_condition;
  pthread_cond_t consumer_condition;

  // Dynamic array of thread identifiers for producer and consumer threads.
  // Use these identifiers while creating the threads and joining the threads.
  pthread_t *producer_threads;
  pthread_t *consumer_threads;

  // Arrays for holding and accessing shared memory for each thread
  SharedMemory* producer_memory;
  SharedMemory* consumer_memory;

  int active_producer_count;
  int active_consumer_count;

  void initSharedMem(SharedMemory* mem);

public:
  // The following 6 methods should be defined in the implementation file (solution.cpp)
  ProducerConsumerProblem(long _n_items, int _n_producers, int _n_consumers,
                          long _queue_size);
  ~ProducerConsumerProblem();
  void startProducers();
  void startConsumers();
  void joinProducers();
  void joinConsumers();
  void printStats();
};
