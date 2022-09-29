LIBS = -lerpc -lpthread -lnuma -ldl -ldpdk -lgflags
ERPC_DIR = /proj/sequencer/eRPC
INCLUDES = -I $(ERPC_DIR)/src -I /usr/include/dpdk
FLAGS = -O3 -g -rdynamic -std=c++11 -DDPDK=true -march=native

server: server.cc server.h ../common.h ../logger.h
	bash ../ltomake server

clean:
	rm server