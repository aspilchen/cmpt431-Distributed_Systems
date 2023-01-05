#include "core/graph.h"
#include "core/utils.h"
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>
#include <vector>

struct GlobalMem {
  const Graph* g;
} global_mem;


struct ThreadMem {
  ThreadMem() 
  :triangle_count{0}, time_taken{0}, start{0}, end{0}, n_vertices{0}, n_edges{0}
  {}

  const Graph* g;
  long triangle_count;
  double time_taken;
  uintV start;
  uintV end;
  uintV n_vertices;
  uintE n_edges;
};

using ThreadVec = std::vector<pthread_t>;

uintV countTriangles(uintV *array1, uintE len1, uintV *array2, uintE len2,
                     uintV u, uintV v) {

  uintE i = 0, j = 0; // indexes for array1 and array2
  uintV count = 0;

  if (u == v)
    return count;

  while ((i < len1) && (j < len2)) {
    if (array1[i] == array2[j]) {
      if ((array1[i] != u) && (array1[i] != v)) {
        count++;
      }
      i++;
      j++;
    } else if (array1[i] < array2[j]) {
      i++;
    } else {
      j++;
    }
  }
  return count;
}

void* trianglesThreadJob(void* _arg) {
  ThreadMem* mem = static_cast<ThreadMem*>(_arg);
  timer t;
  t.start();
  // Process each edge <u,v>
  for (uintV u = mem->start; u < mem->end; u++) {
    mem->n_vertices++;
    // For each outNeighbor v, find the intersection of inNeighbor(u) and
    // outNeighbor(v)
    uintE out_degree = global_mem.g->vertices_[u].getOutDegree();
    mem->n_edges += out_degree;
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = global_mem.g->vertices_[u].getOutNeighbor(i);
      mem->triangle_count += countTriangles(global_mem.g->vertices_[u].getInNeighbors(),
                                       global_mem.g->vertices_[u].getInDegree(),
                                       global_mem.g->vertices_[v].getOutNeighbors(),
                                       global_mem.g->vertices_[v].getOutDegree(), u, v);
    }
  }
  mem->time_taken = t.stop();
  return NULL;
}

using MemVec = std::vector<ThreadMem>;

void printThreadStats(const MemVec& mem) {
  std::cout << "thread_id, num_vertices, num_edges, triangle_count, time_taken\n";
  uint id = 0;
  for(auto& i: mem) {
    std::cout << id << ", ";
    std::cout << i.n_vertices << ", ";
    std::cout << i.n_edges << ", ";
    std::cout << i.triangle_count << ", ";
    std::cout << i.time_taken << "\n";
    id++;
  }
}

double partitionWorkByEdge(MemVec& mem, uintE n) {
  timer t;
  t.start();

  uintE job_size = n / mem.size();
  uintE edge_count = 0;
  uintV start = 0;
  uintV end = 0;
  uintV out_degree = 0;

  for(auto& i: mem) {
    while(edge_count < job_size) {
      edge_count += global_mem.g->vertices_[end].getOutDegree();
      end++;
    }
    i.start = start;
    i.end = end;
    start = end;
    edge_count = 0;
  }
  mem.back().end = global_mem.g->n_;
  return t.stop();
}

double partitionWorkByVertex(MemVec& mem, uintV n) {
  timer t;
  t.start();
  uintV job_size = n / mem.size();
  uintV start = 0;
  uintV end = job_size;

  for(auto& i: mem) {
    i.start = start;
    i.end = end;
    start = end;
    end += job_size;
  }
  mem.back().end = n;

  return t.stop();
}

void startThreads(ThreadVec& threads, MemVec& mem) {
  uint id = 0;
  for(auto& i: threads) {
    pthread_create(&i, NULL, trianglesThreadJob, static_cast<void*>(&mem[id]));
    id++;
  }
}

void joinThreads(ThreadVec& threads) {
  for(auto& i: threads) {
    pthread_join(i, NULL);
  }
}

enum Strategy {
  s1,
  s2
};

void triangleCountParallel(Graph& g, uint n_workers, Strategy strat) {
  global_mem.g = &g;
  long triangle_count = 0;
  double partition_time = 0.0;
  double time_taken = 0.0;
  timer t1;
  ThreadVec threads(n_workers);
  MemVec thread_mem(n_workers);

  t1.start();

  if(strat == Strategy::s1)
    partition_time = partitionWorkByVertex(thread_mem, g.n_);
  else
    partition_time = partitionWorkByEdge(thread_mem, g.m_);


  startThreads(threads, thread_mem);
  joinThreads(threads);

  for(auto& i: thread_mem)
    triangle_count += i.triangle_count;

  time_taken = t1.stop();

  printThreadStats(thread_mem);

  // Print the overall statistics
  std::cout << "Number of triangles : " << triangle_count << "\n";
  std::cout << "Number of unique triangles : " << triangle_count / 3 << "\n";
  std::cout << "Partitioning time (in seconds) : " << std::setprecision(TIME_PRECISION)
            << partition_time << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
}

void triangleCountSerial(Graph &g) {
  uintV n = g.n_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;

  // -------------------------------------------------------------------
  t1.start();
  // Process each edge <u,v>
  for (uintV u = 0; u < n; u++) {
    // For each outNeighbor v, find the intersection of inNeighbor(u) and
    // outNeighbor(v)
    uintE out_degree = g.vertices_[u].getOutDegree();
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = g.vertices_[u].getOutNeighbor(i);
      triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                       g.vertices_[u].getInDegree(),
                                       g.vertices_[v].getOutNeighbors(),
                                       g.vertices_[v].getOutDegree(), u, v);
    }
  }
  time_taken = t1.stop();
  // -------------------------------------------------------------------

  // Print the overall statistics
  std::cout << "Number of triangles : " << triangle_count << "\n";
  std::cout << "Number of unique triangles : " << triangle_count / 3 << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
}

int main(int argc, char *argv[]) {
  cxxopts::Options options(
      "triangle_counting_serial",
      "Count the number of triangles using serial and parallel execution");
  options.add_options(
      "custom",
      {
          {"nWorkers", "Number of workers",
           cxxopts::value<uint>()->default_value(DEFAULT_NUMBER_OF_WORKERS)},
          {"strategy", "Strategy to be used",
           cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  uint strategy = cl_options["strategy"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";
  std::cout << "Task decomposition strategy : " << strategy << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";

  switch (strategy) {
  case 0:
    std::cout << "\nSerial\n";
    triangleCountSerial(g);
    break;
  case 1:
    std::cout << "\nVertex-based work partitioning\n";
    triangleCountParallel(g, n_workers, Strategy::s1);
    break;
  case 2:
    std::cout << "\nEdge-based work partitioning\n";
    triangleCountParallel(g, n_workers, Strategy::s2);
    break;
  default:
    break;
  }

  return 0;
}
