CC = g++
CFLAGS = -std=c++11 -Wall
TAR = tar
TARFLAGS = cvf
TARNAME = ex3.tar
TARSRCS = MapReduceFramework.cpp Barrier.cpp Barrier.h Makefile README

all: MapReduceFramework.o
	ar rcs libMapReduceFramework.a MapReduceFramework.o Barrier.o

MapReduceFramework.o: MapReduceFramework.cpp MapReduceFramework.h MapReduceClient.h Barrier.cpp Barrier.h
	$(CC) $(CFLAGS) MapReduceFramework.cpp -c &&\
	$(CC) $(CFLAGS) Barrier.cpp -c



clean:
	rm MapReduceFramework.o Barrier.o libMapReduceFramework.a ex3.tar

tar:
	$(TAR) $(TARFLAGS) $(TARNAME) $(TARSRCS)

.PHONY: clean, all, tar
