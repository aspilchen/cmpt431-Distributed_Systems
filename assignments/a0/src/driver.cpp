#include "core/utils.h"
#include "solution.cpp"

int main(int argc, char *argv[]) {
  cxxopts::Options options("test",
                           "Driver Program for producer consumer problem");
  options.add_options(
      "",
      {
          {"nItems", "Number of items to be produced",
           cxxopts::value<long>()->default_value(DEFAULT_NUMBER_OF_ITEMS)},
          {"nProducers", "Number of producers",
           cxxopts::value<int>()->default_value(DEFAULT_NUMBER_OF_PRODUCERS)},
          {"nConsumers", "Number of consumers",
           cxxopts::value<int>()->default_value(DEFAULT_NUMBER_OF_CONSUMERS)},
          {"bufferSize", "Size of the buffer used for produced items",
           cxxopts::value<long>()->default_value(DEFAULT_QUEUE_SIZE)},
      });

  auto cl_options = options.parse(argc, argv);
  long n_items = cl_options["nItems"].as<long>();
  int n_producers = cl_options["nProducers"].as<int>();
  int n_consumers = cl_options["nConsumers"].as<int>();
  long queue_size = cl_options["bufferSize"].as<long>();

  ProducerConsumerProblem solution(n_items, n_producers, n_consumers,
                                   queue_size);

  timer t1;
  t1.start();
  solution.startProducers();
  solution.startConsumers();
  solution.joinProducers();
  solution.joinConsumers();
  double time_taken = t1.stop();

  solution.printStats();

  std::cout << "Time taken: " << time_taken << "\n";
  pthread_exit(NULL);

  return 0;
}