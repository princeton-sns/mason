LIBS = -lerpc -lpthread -lnuma -ldl -ldpdk -lgflags
ERPC_DIR = /proj/sequencer/eRPC
INCLUDES = -I $(ERPC_DIR)/src -I /usr/include/dpdk
FLAGS = -O3 -g -rdynamic -std=c++11 -DDPDK=true -march=native

corfu_server: corfu_server.cc corfu_server.h ../common.h ../logger.h
	bash ../ltomake corfu_server

slowcorfu_server: corfu_server.cc corfu_server.h ../common.h
	g++ -O3 -g -rdynamic -std=c++11 -o corfu_server corfu_server.cc $(INCLUDES) -L $(ERPC_DIR)/build $(LIBS) -DDPDK=true -DCOMMON_DIR=$(shell cd ../; pwd) -march=native

clean:
	rm corfu_server
