#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <vector>

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
typedef float PageRankType;
#endif

// Page rank class provides thread safe write
class PageRank {
public:
  PageRank()
  :rank{0}
  {}

  PageRank(const PageRank& pr) {}

  PageRankType read() {
    return readImpl();
  }

  void write(PageRankType r) {
    writeImpl(r);
  }

  void incrementBy(PageRankType r) {
    incrementImpl(r);
  }

private:
  PageRankType readImpl() {
    return rank;
  }

  void writeImpl(PageRankType r) {
    mtx.lock();
    rank = r;
    mtx.unlock();
  }

  void incrementImpl(PageRankType r) {
    mtx.lock();
    rank += r;
    mtx.unlock();
  }

  PageRankType rank;
  std::mutex mtx;
};

// This is just to make life easier for the sake of assignment..
// Would never do a global thing like this in production
struct SharedMem {
  Graph* g;
  CustomBarrier* barrier;
  PageRankType* pr_curr;
  PageRank* pr_next;
  int max_iters = 0;
} shared_mem;

struct ThreadArg {
  uintV start;
  uintV end;
  double time_taken;
};

#define GRAPH (*shared_mem.g)
#define BARRIER (*shared_mem.barrier)
#define PR_CURR shared_mem.pr_curr
#define PR_NEXT shared_mem.pr_next
#define MTX shared_mem.mtx
#define ACTIVE_THREADS shared_mem.active_threads
#define MAX_ITERS shared_mem.max_iters

void prThreadSum(const ThreadArg& arg) {
  uintV v = 0;
  uintE out_degree = 0;
  PageRankType sum = 0;

  for(uintV u = arg.start; u < arg.end; u++) {
    out_degree = GRAPH.vertices_[u].getOutDegree();
    for(uintV j = 0; j < out_degree; j++) {
      v = (GRAPH.vertices_[u].getOutNeighbor(j));
      PR_NEXT[v].incrementBy(PR_CURR[u] / out_degree);
    }
  }
}

void prThreadRank(const ThreadArg& arg) {
  for(uintV u = arg.start; u < arg.end; u++) {
    PR_CURR[u] = PAGE_RANK(PR_NEXT[u].read());
    PR_NEXT[u].write(0);
  }
}

// I really wanted to use coroutines here but c++14 doesnt seem to have them
void pageRank(const ThreadArg& arg) {
  uintV u = 0;
  uintV v = 0;
  uintE out_degree = 0;

  for(int i = 0; i < MAX_ITERS; i++) {
    BARRIER.wait();
    prThreadSum(arg);
    BARRIER.wait();
    prThreadRank(arg);
  }
}

void* prThread(void* _arg) {
  auto* arg = static_cast<ThreadArg*>(_arg);
  timer t;
  t.start();
  pageRank(*arg);
  arg->time_taken = t.stop();
  return NULL;
}

void pageRankParallel(Graph &g, int max_iters, uint n_workers) {
  CustomBarrier barrier(n_workers);

  // set up dynamic shared memory
  shared_mem.g = &g;
  shared_mem.barrier = &barrier;
  MAX_ITERS = max_iters;

  PR_CURR = new PageRankType[g.n_];
  PR_NEXT = new PageRank[g.n_];
  for(uintV i = 0; i < g.n_; i++) {
    PR_CURR[i] = INIT_PAGE_RANK;
    PR_NEXT[i].write(0);
  }

  std::vector<pthread_t> threads(n_workers);
  std::vector<ThreadArg> thread_args(n_workers);

  uintV work_size = GRAPH.n_ / n_workers;
  uintV start = 0;
  uintV end = work_size;
  for(uint i = 0; i < n_workers - 1; i++) {
    thread_args[i].start = start;
    thread_args[i].end = end;
    thread_args[i].time_taken = 0.0;
    start = end;
    end += work_size;
  }
  thread_args[n_workers - 1].start = start;
  thread_args[n_workers - 1].end = GRAPH.n_;
  thread_args[n_workers - 1].time_taken = 0.0;

  std::vector<double> thread_times(n_workers, 0.0);

  //----------------------------------------
  // start threads
  timer t;
  t.start();
  for(uint i = 0; i < n_workers; i++) {
    pthread_create(&threads[i], NULL, prThread, static_cast<void*>(&thread_args[i]));
  }

  // join threads
  for(auto& i: threads) {
    pthread_join(i, NULL);
  }
  double time_taken = t.stop();
  //----------------------------------------

  PageRankType sum_of_page_ranks = 0;
  for(uintV i = 0; i < g.n_; i++) {
    sum_of_page_ranks += PR_CURR[i];
  }

  std::cout << "thread_id, time_taken\n";
  for(uint i = 0; i < n_workers; i++) {
    std::cout << i << ", " << thread_args[i].time_taken << '\n';
  }
  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
  std::cout << "Time taken (in seconds) : " << time_taken << "\n";
  delete[] PR_CURR;
  delete[] PR_NEXT;
}

int main(int argc, char *argv[]) {
  cxxopts::Options options(
      "page_rank_push",
      "Calculate page_rank using serial and parallel execution");
  options.add_options(
      "",
      {
          {"nWorkers", "Number of workers",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
          {"nIterations", "Maximum number of iterations",
           cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
  std::cout << "Using INT\n";
#else
  std::cout << "Using FLOAT\n";
#endif
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";
  pageRankParallel(g, max_iterations, n_workers);

  return 0;
}
