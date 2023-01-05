#include <iostream>
#include <mpi.h>
#include <cstdio>
#include <vector>
#include "core/utils.h"
#include "core/graph.h"


struct PData {
  PData()
  :world_rank{0},
  start{0},
  end{0},
  edges{0},
  triangles{0},
  time_taken{0.0}
  {}

  void print() const {
    std::printf("%u, %u, %ld, %f\n", world_rank, edges, triangles, time_taken);
  }

  void sendStart(const int dest) const {
    MPI_Send(&start, 1, MPI_UNSIGNED, dest, 0, MPI_COMM_WORLD);
  }

  void sendEnd(const int dest) const {
    MPI_Send(&end, 1, MPI_UNSIGNED, dest, 0, MPI_COMM_WORLD);
  }

  void sendTriangles(const int dest) const {
    MPI_Send(&triangles, 1, MPI_LONG, dest, 0, MPI_COMM_WORLD);
  }

  void rcvStart(const int src) {
    MPI_Recv(&start, 1, MPI_UNSIGNED, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  void rcvEnd(const int src) {
    MPI_Recv(&end, 1, MPI_UNSIGNED, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  void rcvTriangles(const int src) {
    MPI_Recv(&triangles, 1, MPI_LONG, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
  }

  int world_rank;
  uintV start;
  uintV end;
  uintE edges;
  long triangles;
  double time_taken;
};

using PdataVec = std::vector<PData>;

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
      } else {
        // triangle with self-referential edge -> ignore
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


void doCount(const Graph &g, PData& data) {
    uintV n = g.n_;
    uintE edge_count = 0;
    long triangle_count = 0;
    for (uintV u = data.start; u < data.end; u++) {
        uintE out_degree = g.vertices_[u].getOutDegree();
        edge_count += out_degree;
        for (uintE i = 0; i < out_degree; i++) {
            uintV v = g.vertices_[u].getOutNeighbor(i);
            triangle_count += countTriangles(g.vertices_[u].getInNeighbors(),
                                             g.vertices_[u].getInDegree(),
                                             g.vertices_[v].getOutNeighbors(),
                                             g.vertices_[v].getOutDegree(),
                                             u,
                                             v);
        }
    }
    
  data.triangles = triangle_count;
  data.edges = edge_count;
}


// Partition vertices by edge count
void partitionByEdges(const Graph& g, PdataVec& data) {
  uintV start = 0;
  uintV end = 0;
  uintE edge_count = 0;
  uintE max_edges = g.m_ / data.size();

  for(auto& i: data) {
    edge_count = 0;
    start = end;

    while(edge_count < max_edges) {
      edge_count += g.vertices_[end].getOutDegree();
      end += 1;
    }

    i.start = start;
    i.end = end;
  }

  data.back().end = g.n_;
}


void sendJobData(PdataVec& data) {
  timer t;
  t.start();
  for(int i = 1; i < data.size(); i++) {
    data[i].sendStart(i);
    data[i].sendEnd(i);
  }
  data[0].time_taken += t.stop();
}

void getJob(PData& data) {
  timer t;
  t.start();
  MPI_Comm_rank(MPI_COMM_WORLD, &data.world_rank);
  data.rcvStart(0);
  data.rcvEnd(0);
  data.time_taken += t.stop();
}

void reportData(PData& data) {
  timer t;
  t.start();
  data.sendTriangles(0);
  data.time_taken = t.stop();
}

void rcvCounts(PdataVec& data) {
  timer t;
  t.start();
  for(int i = 1; i < data.size(); i++) {
    data[i].rcvTriangles(i);
  }
  data[0].time_taken += t.stop();
}

void printStatHeader() {
  std::printf("rank, edges, triangle_count, communication_time\n");
}

void printTotalResults(const long triangle_count, const double time_taken) {
    std::printf("Number of triangles : %ld\n", triangle_count);
    std::printf("Number of unique triangles : %ld\n", triangle_count / 3);
    std::printf("Time taken (in seconds) : %f\n", time_taken);
}

long sumCounts(PdataVec& data) {
  long count = 0;
  timer t;
  t.start();
  for(auto& i: data)
    count += i.triangles;
  data[0].time_taken += t.stop();
  return count;
}

void triangleCountingMaster(const Graph& g) {
  timer t_all;
  t_all.start();
  int world_size;
  long triangle_count = 0;
  double time_taken = 0;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  PdataVec data(world_size);
  printStatHeader();
  partitionByEdges(g, data);
  sendJobData(data);
  doCount(g, data[0]);
  rcvCounts(data);
  triangle_count = sumCounts(data);
  data[0].print();
  time_taken = t_all.stop();
  printTotalResults(triangle_count, time_taken);
}

void triangleCountingSlave(const Graph& g) {
  PData data;
  getJob(data);
  doCount(g, data);
  reportData(data);
  data.print();
}

void triangleCountParallel(const Graph& g) {
  int world_rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  if(world_rank == 0) {
    triangleCountingMaster(g);
  }
  else {
    triangleCountingSlave(g);
  }

}

void printStartDetails(const uint strategy) {
  int world_size;
  int world_rank;
  MPI_Comm_size(MPI_COMM_WORLD, &world_size);
  MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

  if(world_rank == 0) {
    std::printf("World size : %d\n", world_size);
    std::printf("Communication strategy : %d\n", strategy);
  }
}

int main(int argc, char *argv[])
{
    cxxopts::Options options("triangle_counting_serial", "Count the number of triangles using serial and parallel execution");
    options.add_options("custom", {
                                      {"strategy", "Strategy to be used", cxxopts::value<uint>()->default_value(DEFAULT_STRATEGY)},
                                      {"inputFile", "Input graph file path", cxxopts::value<std::string>()->default_value("/scratch/input_graphs/roadNet-CA")},
                                  });

    auto cl_options = options.parse(argc, argv);
    uint strategy = cl_options["strategy"].as<uint>();
    std::string input_file_path = cl_options["inputFile"].as<std::string>();

    // Get the world size and print it out here
    MPI_Init(NULL, NULL);
    printStartDetails(strategy);
    Graph g;
    g.readGraphFromBinary<int>(input_file_path);

    triangleCountParallel(g);
    MPI_Finalize();
    return 0;
}

