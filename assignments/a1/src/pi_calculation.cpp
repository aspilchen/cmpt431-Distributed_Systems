#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <ctime>

#define sqr(x) ((x) * (x))
#define DEFAULT_NUMBER_OF_POINTS "12345678"

struct SharedMemory {
  uint n_points;
  uint random_seed;
  uint n_generated;
  uint circle_count;
  double time_taken;
};

uint c_const = (uint)RAND_MAX + (uint)1;
inline double get_random_coordinate(uint *random_seed) {
  return ((double)rand_r(random_seed)) / c_const;
}

uint get_points_in_circle(uint n, uint random_seed) {
  uint circle_count = 0;
  double x_coord, y_coord;
  for (uint i = 0; i < n; i++) {
    x_coord = (2.0 * get_random_coordinate(&random_seed)) - 1.0;
    y_coord = (2.0 * get_random_coordinate(&random_seed)) - 1.0;
    if ((sqr(x_coord) + sqr(y_coord)) <= 1.0)
      circle_count++;
  }
  return circle_count;
}

void* get_points(void* _arg) {
  timer t;
  t.start();
  SharedMemory* mem = static_cast<SharedMemory*>(_arg);
  mem->circle_count = get_points_in_circle(mem->n_points, mem->random_seed);
  mem->time_taken = t.stop();
  return NULL;
}

void set_up_shared_mem(SharedMemory* mem, uint n_points, uint n_workers) {
  uint thread_points = n_points / n_workers;
  uint remainder = n_points - (thread_points * n_workers); 
  srand((unsigned) time(NULL));
  for(uint i = 0; i < n_workers; i++) {
    mem[i].n_points = thread_points;
    mem[i].random_seed = rand();
    mem[i].circle_count = 0;
    mem[i].time_taken = 0.0;
  }
  mem[n_workers-1].n_points += n_points - (thread_points * n_workers);
}

void start_threads(pthread_t* threads, SharedMemory* mem, uint n_workers) {
  for(uint i = 0; i < n_workers; i++) {
    pthread_create(&threads[i], NULL, get_points, static_cast<void*>(&mem[i]));
  }
}

void join_threads(pthread_t* threads, uint n_workers) {
  for(uint i = 0; i < n_workers; i++) {
    pthread_join(threads[i], NULL);
  }
}

double calc_pi(SharedMemory* mem, uint n_workers) {
  uint circle_points = 0;
  uint n_points = 0;

  for(uint i = 0; i < n_workers; i++) {
    circle_points += mem[i].circle_count;
    n_points += mem[i].n_points;
  }

  return 4.0 * (double)circle_points / (double)n_points;
}

void piCalculation(uint n_points, uint n_workers) {
  timer serial_timer;
  double time_taken = 0.0;
  pthread_t* threads = new pthread_t[n_workers];
  SharedMemory* shared_mem = new SharedMemory[n_workers];
  set_up_shared_mem(shared_mem, n_points, n_workers);

  serial_timer.start();
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  start_threads(threads, shared_mem, n_workers);
  join_threads(threads, n_workers);
  double pi_value = calc_pi(shared_mem, n_workers);
  // -------------------------------------------------------------------
  time_taken = serial_timer.stop();

  uint circle_points = 0;
  uint total_generated = 0;
  for(uint i = 0; i < n_workers; i++) {
    circle_points += shared_mem[i].circle_count;
    total_generated += shared_mem[i].n_points;
  }
  // Print the above statistics for each thread
  // Example output for 2 threads:
  // thread_id, points_generated, circle_points, time_taken
  // 1, 100, 90, 0.12
  // 0, 100, 89, 0.12
  std::cout << "thread_id, points_generated, circle_points, time_taken\n";
  for(uint i = 0; i < n_workers; i++) {
    std::cout << i << ", ";
    std::cout << shared_mem[i].n_points << ", ";
    std::cout << shared_mem[i].circle_count << ", ";
    std::cout << std::setprecision(TIME_PRECISION) << shared_mem[i].time_taken << "\n";
  }

  // Print the overall statistics
  std::cout << "Total points generated : " << total_generated << "\n";
  std::cout << "Total points in circle : " << circle_points << "\n";
  std::cout << "Result : " << std::setprecision(VAL_PRECISION) << pi_value
            << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";

  delete[] threads;
  delete[] shared_mem;
}

int main(int argc, char *argv[]) {
  // Initialize command line arguments
  cxxopts::Options options("pi_calculation",
                           "Calculate pi using serial and parallel execution");
  options.add_options(
      "custom",
      {
          {"nPoints", "Number of points",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_POINTS)},
          {"nWorkers", "Number of workers",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_points = cl_options["nPoints"].as<uint>();
  uint n_workers = cl_options["nWorkers"].as<uint>();
  std::cout << std::fixed;
  std::cout << "Number of points : " << n_points << "\n";
  std::cout << "Number of workers : " << n_workers << "\n";

  piCalculation(n_points, n_workers);

  return 0;
}
