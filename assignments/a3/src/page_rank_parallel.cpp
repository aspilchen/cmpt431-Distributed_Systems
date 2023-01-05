#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <vector>
#include <atomic>

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


void pageRankSerial(Graph &g, int max_iters) {
  uintV n = g.n_;

  PageRankType *pr_curr = new PageRankType[n];
  PageRankType *pr_next = new PageRankType[n];

  for (uintV i = 0; i < n; i++) {
    pr_curr[i] = INIT_PAGE_RANK;
    pr_next[i] = 0.0;
  }

  // Push based pagerank
  timer t1;
  double time_taken = 0.0;
  // Create threads and distribute the work across T threads
  // -------------------------------------------------------------------
  t1.start();
  for (int iter = 0; iter < max_iters; iter++) {
    // for each vertex 'u', process all its outNeighbors 'v'
    for (uintV u = 0; u < n; u++) {
      uintE out_degree = g.vertices_[u].getOutDegree();
      for (uintE i = 0; i < out_degree; i++) {
        uintV v = g.vertices_[u].getOutNeighbor(i);
        pr_next[v] += (pr_curr[u] / out_degree);
      }
    }
    for (uintV v = 0; v < n; v++) {
      pr_next[v] = PAGE_RANK(pr_next[v]);

      // reset pr_curr for the next iteration
      pr_curr[v] = pr_next[v];
      pr_next[v] = 0.0;
    }
  }
  time_taken = t1.stop();
  // -------------------------------------------------------------------

  PageRankType sum_of_page_ranks = 0;
  for (uintV u = 0; u < n; u++) {
    sum_of_page_ranks += pr_curr[u];
  }
  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
  std::cout << "Time taken (in seconds) : " << time_taken << "\n";
  delete[] pr_curr;
  delete[] pr_next;
}


// This is just to make life easier for the sake of assignment..
// Would never do a global thing like this in production
struct SharedMem {
  Graph* g;
  CustomBarrier* barrier;
  PageRankType* pr_curr;
  std::atomic<PageRankType>* pr_next;
  int max_iters = 0;
} shared_mem;

struct ThreadMem {
  ThreadMem() 
  :start{0}, end{0}, n_vertices{0}, n_edges{0}, wait_time_one{0.0}, wait_time_two{0.0}, time_taken{0.0}
  {}

  uintV start;
  uintV end;
  uintV n_vertices;
  uintE n_edges;
  double wait_time_one;
  double wait_time_two;
  double time_taken;
};

#define GRAPH (*shared_mem.g)
#define BARRIER (*shared_mem.barrier)
#define PR_CURR shared_mem.pr_curr
#define PR_NEXT shared_mem.pr_next
#define MTX shared_mem.mtx
#define ACTIVE_THREADS shared_mem.active_threads
#define MAX_ITERS shared_mem.max_iters

void prThreadSum(ThreadMem& arg) {
  uintV v = 0;
  uintE out_degree = 0;
  PageRankType value = 0;
  PageRankType expected = 0;

  for(uintV u = arg.start; u < arg.end; u++) {
    out_degree = GRAPH.vertices_[u].getOutDegree();
    arg.n_vertices++;
    arg.n_edges += out_degree;
    for(uintV j = 0; j < out_degree; j++) {
      v = GRAPH.vertices_[u].getOutNeighbor(j);
      expected = PR_NEXT[v].load();
      value = (PR_CURR[u] / out_degree);
      while(!PR_NEXT[v].compare_exchange_strong(expected, expected + value));
    }
  }
}

void prThreadRank(const ThreadMem& arg) {
  for(uintV u = arg.start; u < arg.end; u++) {
    PR_CURR[u] = PAGE_RANK(PR_NEXT[u].load());
    PR_NEXT[u].store(0);
  }
}

// I really wanted to use coroutines here but c++14 doesnt seem to have them
void pageRank(ThreadMem& arg) {
  uintV u = 0;
  uintV v = 0;
  uintE out_degree = 0;
  timer t;

  for(int i = 0; i < MAX_ITERS; i++) {

    t.start();
    BARRIER.wait();
    arg.wait_time_one += t.stop();

    prThreadSum(arg);

    t.start();
    BARRIER.wait();
    arg.wait_time_two += t.stop();

    prThreadRank(arg);
  }
}

void* prThread(void* _arg) {
  auto* arg = static_cast<ThreadMem*>(_arg);
  timer t;
  t.start();
  pageRank(*arg);
  arg->time_taken = t.stop();
  return NULL;
}

double partitionByVertices(std::vector<ThreadMem>& mem, uint n_workers) {
  timer t;
  t.start();
  uintV work_size = GRAPH.n_ / n_workers;
  uintV start = 0;
  uintV end = work_size;

  for(auto& i: mem) {
    i.start = start;
    i.end = end;
    start = end;
    end += work_size;
  }
  mem.back().end = GRAPH.n_;

  return t.stop();
}

double partitionByEdges(std::vector<ThreadMem>& mem, uint n_workers) {
  timer t;
  t.start();
  uintE work_size = GRAPH.m_ / n_workers;
  uintE edge_count = 0;
  uintV start = 0;
  uintV end = 0;

  for(auto& i: mem) {
    while(edge_count < work_size) {
      edge_count += GRAPH.vertices_[end].getOutDegree();
      end++;
    }
    i.start = start;
    i.end = end;
    start = end;
    edge_count = 0;
  }
  mem.back().end = GRAPH.n_;

  return t.stop();
}


void printThreadStats(const std::vector<ThreadMem>& mem) {
  uint id = 0;

  std::cout << "thread_id, num_vertices, num_edges, barrier1_time, barrier2_time, total_time\n";
  for(auto& i: mem) {
    std::cout << id << ", ";
    std::cout << i.n_vertices << ", ";
    std::cout << i.n_edges << ", ";
    std::cout << i.wait_time_one << ", ";
    std::cout << i.wait_time_two << ", ";
    std::cout << i.time_taken << "\n";
    id++;
  }
}

enum Strategy {
  s1,
  s2
};

void pageRankParallel(Graph &g, int max_iters, uint n_workers, Strategy s) {
  CustomBarrier barrier(n_workers);
  double partition_time = 0;

  // set up dynamic shared memory
  shared_mem.g = &g;
  shared_mem.barrier = &barrier;
  MAX_ITERS = max_iters;

  PR_CURR = new PageRankType[g.n_];
  PR_NEXT = new std::atomic<PageRankType>[g.n_];
  for(uintV i = 0; i < g.n_; i++) {
    PR_CURR[i] = INIT_PAGE_RANK;
    PR_NEXT[i].store(0);
  }

  std::vector<pthread_t> threads(n_workers);
  std::vector<ThreadMem> thread_mem(n_workers);

  timer t;
  t.start();

  if(s == Strategy::s1)
    partition_time = partitionByVertices(thread_mem, n_workers);
  else
    partition_time = partitionByEdges(thread_mem, n_workers);

  //----------------------------------------
  // start threads
  for(uint i = 0; i < n_workers; i++) {
    pthread_create(&threads[i], NULL, prThread, static_cast<void*>(&thread_mem[i]));
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

  printThreadStats(thread_mem);

  std::cout << "Sum of page rank : " << sum_of_page_ranks << "\n";
  std::cout << "Partitioning time (in seconds) : " << partition_time << "\n";
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
          {"strategy", "Strategy to be used",
           cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  uint strategy = cl_options["strategy"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
  std::cout << "Using INT\n";
#else
  std::cout << "Using FLOAT\n";
#endif
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";
  std::cout << "Task decomposition strategy : " << strategy << "\n";
  std::cout << "Iterations : " << max_iterations << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";
  switch (strategy) {
  case 0:
    std::cout << "\nSerial\n";
    pageRankSerial(g, max_iterations);
    break;
  case 1:
    std::cout << "\nVertex-based work partitioning\n";
    pageRankParallel(g, max_iterations, n_workers, Strategy::s1);
    break;
  case 2:
    std::cout << "\nEdge-based work partitioning\n";
    pageRankParallel(g, max_iterations, n_workers, Strategy::s2);
    break;
  default:
    break;
  }

  return 0;
}