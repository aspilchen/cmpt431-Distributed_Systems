#compiler setup
CXX = g++
CXXFLAGS = -std=c++14 -march=native -pthread -O3 

COMMON= core/utils.h core/cxxopts.h core/get_time.h core/circular_queue.h
SOLUTION= solution.cpp solution.h
ALL= producer_consumer

all : $(ALL)

producer_consumer: driver.cpp ${COMMON} ${SOLUTION}
	$(CXX) $(CXXFLAGS) $< -o $@

.PHONY : clean

clean :
	rm -f *.o *.obj $(ALL)