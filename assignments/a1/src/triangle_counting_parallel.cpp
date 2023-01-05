#include "core/graph.h"
#include "core/utils.h"
#include <future>
#include <iomanip>
#include <iostream>
#include <stdlib.h>
#include <thread>

struct SharedMemory {
  Graph* g;
  long triangle_count;
  double time_taken;
  uintV start_vertex;
  uintV end_vertex;
};

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
  SharedMemory* mem = static_cast<SharedMemory*>(_arg);
  timer t;
  t.start();
  // Process each edge <u,v>
  for (uintV u = mem->start_vertex; u < mem->end_vertex; u++) {
    // For each outNeighbor v, find the intersection of inNeighbor(u) and
    // outNeighbor(v)
    uintE out_degree = mem->g->vertices_[u].getOutDegree();
    for (uintE i = 0; i < out_degree; i++) {
      uintV v = mem->g->vertices_[u].getOutNeighbor(i);
      mem->triangle_count += countTriangles(mem->g->vertices_[u].getInNeighbors(),
                                       mem->g->vertices_[u].getInDegree(),
                                       mem->g->vertices_[v].getOutNeighbors(),
                                       mem->g->vertices_[v].getOutDegree(), u, v);
    }
  }
  mem->time_taken = t.stop();
  return NULL;
}

void setUpSharedMem(SharedMemory* mem, Graph& g, uint n_workers) {
  uintV n_vertex_per_thread = g.n_ / n_workers;
  for(uint i = 0; i < n_workers; i++) {
    mem[i].g = &g;
    mem[i].triangle_count = 0;
    mem[i].time_taken = 0.0;
    mem[i].start_vertex = i * n_vertex_per_thread;
    mem[i].end_vertex = mem[i].start_vertex + n_vertex_per_thread;
  }
  mem[n_workers - 1].end_vertex = g.n_; // Handle remainders
}

void startThreads(pthread_t* threads, SharedMemory* mem, uint n_workers) {
  for(uint i = 0; i < n_workers; i++) {
    pthread_create(&threads[i], NULL, trianglesThreadJob, static_cast<void*>(&mem[i]));
  }
}

void joinThreads(pthread_t* threads, uint n_workers) {
  for(uint i = 0; i < n_workers; i++) {
    pthread_join(threads[i], NULL);
  }
}

void triangleCountParallel(Graph& g, uint n_workers) {
  uintV n = g.n_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;
  pthread_t* threads = new pthread_t[n_workers];
  SharedMemory* shared_mem = new SharedMemory[n_workers];
  setUpSharedMem(shared_mem, g, n_workers);

  t1.start();
  // -------------------------------------------------------------------
  startThreads(threads, shared_mem, n_workers);
  joinThreads(threads, n_workers);

  for(uint i = 0; i < n_workers; i++) {
    triangle_count += shared_mem[i].triangle_count;
  }
  // -------------------------------------------------------------------
  time_taken = t1.stop();

  // -------------------------------------------------------------------
  // Here, you can just print the number of non-unique triangles counted by each
  // thread std::cout << "thread_id, triangle_count, time_taken\n"; Print the
  // above statistics for each thread Example output for 2 threads: thread_id,
  // triangle_count, time_taken 1, 102, 0.12 0, 100, 0.12
  std::cout << "thread_id, triangle_count, time_taken\n";
  for(uint i = 0; i < n_workers; i++) {
    std::cout << i << ", ";
    std::cout << shared_mem[i].triangle_count << ", ";
    std::cout << shared_mem[i].time_taken << "\n";
  }

  // Print the overall statistics
  std::cout << "Number of triangles : " << triangle_count << "\n";
  std::cout << "Number of unique triangles : " << triangle_count / 3 << "\n";
  std::cout << "Time taken (in seconds) : " << std::setprecision(TIME_PRECISION)
            << time_taken << "\n";
  delete[] threads;
  delete[] shared_mem;
}

void triangleCountSerial(Graph &g) {
  uintV n = g.n_;
  long triangle_count = 0;
  double time_taken = 0.0;
  timer t1;

  // The outNghs and inNghs for a given vertex are already sorted

  // Create threads and distribute the work across T threads
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
  // Here, you can just print the number of non-unique triangles counted by each
  // thread std::cout << "thread_id, triangle_count, time_taken\n"; Print the
  // above statistics for each thread Example output for 2 threads: thread_id,
  // triangle_count, time_taken 1, 102, 0.12 0, 100, 0.12

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
          {"inputFile", "Input graph file path",
           cxxopts::value<std::string>()->default_value(
               "/scratch/assignment1/input_graphs/roadNet-CA")},
      });

  auto cl_options = options.parse(argc, argv);
  uint n_workers = cl_options["nWorkers"].as<uint>();
  std::string input_file_path = cl_options["inputFile"].as<std::string>();
  std::cout << std::fixed;
  std::cout << "Number of workers : " << n_workers << "\n";

  Graph g;
  std::cout << "Reading graph\n";
  g.readGraphFromBinary<int>(input_file_path);
  std::cout << "Created graph\n";

  //triangleCountSerial(g);
  triangleCountParallel(g, n_workers);

  return 0;
}
