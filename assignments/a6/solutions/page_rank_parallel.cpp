#include <iostream>
#include <cstdio>
#include <mpi.h>
#include <vector>
#include "core/utils.h"
#include "core/graph.h"

#ifdef USE_INT
#define INIT_PAGE_RANK 100000
#define EPSILON 1000
#define PAGE_RANK(x) (15000 + (5 * x) / 6)
#define CHANGE_IN_PAGE_RANK(x, y) std::abs(x - y)
#define PAGERANK_MPI_TYPE MPI_LONG
#define PR_FMT "%ld"
typedef int64_t PageRankType;
#else
#define INIT_PAGE_RANK 1.0
#define EPSILON 0.01
#define DAMPING 0.85
#define PAGE_RANK(x) (1 - DAMPING + DAMPING * x)
#define CHANGE_IN_PAGE_RANK(x, y) std::fabs(x - y)
#define PAGERANK_MPI_TYPE MPI_FLOAT
#define PR_FMT "%f"
typedef float PageRankType;
#endif

using PRVec = std::vector<PageRankType>;
struct PageRanks {
  PageRanks(uintV size) {
    pr_curr = PRVec(size, INIT_PAGE_RANK);
    pr_next = PRVec(size, 0);
  }

  PRVec pr_curr;
  PRVec pr_next;
};

struct PJob {
  PJob() = default;

  uintV start;
  uintV end;
  uintE edges;
};
using PJVec = std::vector<PJob>;

// Partition vertices by edge count
void partitionByEdges(const Graph& g, PJVec& p_jobs) {
  uintV start = 0;
  uintV end = 0;
  uintE edge_count = 0;
  uintE max_edges = g.m_ / p_jobs.size();

  for(auto& i: p_jobs) {
    edge_count = 0;
    start = end;

    while(edge_count < max_edges) {
      edge_count += g.vertices_[end].getOutDegree();
      end += 1;
    }

    i.start = start;
    i.end = end;
  }

  p_jobs.back().end = g.n_;
}

void prStepOne(const Graph &g, PageRanks &ranks, PJob& job) {
  // for each vertex 'u', process all its outNeighbors 'v'
  for (uintV u = job.start; u < job.end; u++) {
    uintE out_degree = g.vertices_[u].getOutDegree();
    job.edges += out_degree;
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = g.vertices_[u].getOutNeighbor(i);
      ranks.pr_next[v] += (ranks.pr_curr[u] / out_degree);
    }
  }
}

void prStepTwo(const Graph &g, PageRanks &ranks, PJob& job) {
  for (uintV v = 0; v < g.n_; v++) {
    ranks.pr_curr[v] = PAGE_RANK(ranks.pr_next[v]);
    ranks.pr_next[v] = 0.0;
  }
}

double transmitJobs(const PJVec& p_jobs, const int world_size) {
  timer t;
  t.start();
  for(int i = 1; i < world_size; i++) {
    MPI_Send(&p_jobs[i].start, 1, MPI_INT32_T, i, 0, MPI_COMM_WORLD);
    MPI_Send(&p_jobs[i].end, 1, MPI_INT32_T, i, 0, MPI_COMM_WORLD);
  }
  return t.stop();
}

double recieveJob(PJob& p_job) {
  timer t;
  t.start();
  MPI_Recv(&p_job.start, 1, MPI_INT32_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  MPI_Recv(&p_job.end, 1, MPI_INT32_T, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  return t.stop();
}

double syncSub(PageRanks& ranks) {
  timer t;
  t.start();
  MPI_Send(ranks.pr_next.data(), ranks.pr_next.size(), PAGERANK_MPI_TYPE, 0, 0, MPI_COMM_WORLD);
  MPI_Recv(ranks.pr_next.data(), ranks.pr_next.size(), PAGERANK_MPI_TYPE, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  return t.stop();
}

double syncRoot(PageRanks& ranks, const int world_size) {
  timer t;
  t.start();
  PRVec buffer(ranks.pr_next.size());

  for(int i = 1; i < world_size; i++) {
    MPI_Recv(buffer.data(), buffer.size(), PAGERANK_MPI_TYPE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for(uint j = 0; j < buffer.size(); j++) {
      ranks.pr_next[j] += buffer[j];
    }
  }

  for(int i = 1; i < world_size; i++) {
    MPI_Send(ranks.pr_next.data(), ranks.pr_next.size(), PAGERANK_MPI_TYPE, i, 0, MPI_COMM_WORLD);
  }
  return t.stop();
}

PageRankType getLocalSum(const PageRanks& ranks, const PJob& job) {
  PageRankType sum = 0;
  for (size_t i = job.start; i < job.end; i++) {
    sum += ranks.pr_curr[i];
  }
  return sum;
}

double transmitLocalSum(const PageRanks& ranks, const PJob& job) {
  timer t;
  t.start();
  PageRankType sum = getLocalSum(ranks, job);
  MPI_Send(&sum, 1, PAGERANK_MPI_TYPE, 0, 0, MPI_COMM_WORLD);
  return t.stop();
}

double receiveSubSums(PageRankType& sum, const int world_size) {
  timer t;
  t.start();
  PageRankType buffer = 0;
  for(int i = 1; i < world_size; i++) {
    MPI_Recv(&buffer, 1, PAGERANK_MPI_TYPE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    sum += buffer;
  }
  return t.stop();
}

void prRoot(const Graph& g, const uint max_iterations) {
  timer t; t.start();
  double comm_time = 0.0;
  int world_size; MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  PJVec p_jobs(world_size);
  PageRanks ranks(g.n_);
  double time_taken;

  partitionByEdges(g, p_jobs);
  comm_time += transmitJobs(p_jobs, world_size);
  for (uint i = 0; i < max_iterations; i++) {
    prStepOne(g, ranks, p_jobs[0]);
    comm_time += syncRoot(ranks, world_size);
    prStepTwo(g, ranks, p_jobs[0]);
  }
  
  PageRankType sum_of_page_ranks = getLocalSum(ranks, p_jobs[0]);
  comm_time += receiveSubSums(sum_of_page_ranks, world_size);
  time_taken = t.stop();

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  printf("%d, %ld, %f\n", world_rank, p_jobs[0].edges, comm_time);
  std::printf("Sum of page rank : " PR_FMT "\n", sum_of_page_ranks);
  std::printf("Time taken (in seconds) : %f\n", time_taken);
}

void prSub(const Graph& g, const uint max_iterations) {
  double comm_time = 0.0;
  PJob job;
  PageRanks ranks(g.n_);
  recieveJob(job);

  for (uint i = 0; i < max_iterations; i++) {
    prStepOne(g, ranks, job);
    syncSub(ranks);
    prStepTwo(g, ranks, job);
  }

  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
  printf("%d, %ld, &f", world_rank, job.edges, comm_time);
  transmitLocalSum(ranks, job);
}

void pageRankParallel(const Graph& g, const uint max_iterations) {
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  if(world_rank == 0) {
    printf("rank, num_edges, communication_time\n");
    prRoot(g, max_iterations);
  }
  else {
    prSub(g, max_iterations);
  }
}

int main(int argc, char *argv[])
{
  cxxopts::Options options("page_rank_push", "Calculate page_rank using serial and parallel execution");
  options.add_options("", {
                              {"nIterations", "Maximum number of iterations", cxxopts::value<uint>()->default_value(DEFAULT_MAX_ITER)},
                              {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                              {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
                          });

  auto cl_options = options.parse(argc, argv);
  uint strategy = cl_options["strategy"].as<uint>();
  uint max_iterations = cl_options["nIterations"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();

#ifdef USE_INT
  std::printf("Using INT\n");
#else
  std::printf("Using FLOAT\n");
#endif
  MPI_Init(NULL, NULL);
  // Get the world size and print it out here
  // std::printf("World size : %d\n", world_size);
  std::printf("Communication strategy : %d\n", strategy);
  std::printf("Iterations : %d\n", max_iterations);

  Graph g;
  g.readGraphFromBinary<int>(input_file_path);

  pageRankParallel(g, max_iterations);
  MPI_Finalize();
  return 0;
}
