LIBS = -lerpc -lpthread -lnuma -ldl -ldpdk -lgflags -lrt -lraft -lboost_serialization
ERPC_DIR = /proj/sequencer/eRPC
INCLUDES = -I $(ERPC_DIR)/src -I /usr/include/dpdk
FLAGS = -O3 -g -rdynamic -std=c++11 -DDPDK=true -march=native -fno-inline

proxy: proxy.cc proxy.h testing.h timer.h recovery.h gc.h ../common.h ../logger.h ../bitmap.h raft_callbacks.h
	bash ../ltomake proxy

# MUST rename "main()" in proxy.cc for this to work
testproxy: proxy.cc proxy.h testing.h timer.h recovery.h gc.h ../common.h ../logger.h raft_callbacks.h test.cc
	g++ -pg -O3 -rdynamic -std=c++11 -o testproxy test.cc $(INCLUDES) -L $(ERPC_DIR)/build $(LIBS) -DDPDK=true -march=native -DCOMMON_DIR=$(shell cd ..; pwd) -DLOGGER_DIR=$(shell cd ..; pwd)

slowproxy: proxy.cc proxy.h testing.h timer.h recovery.h gc.h ../common.h ../logger.h raft_callbacks.h
	g++ -pg -O3 -rdynamic -std=c++11 -o proxy proxy.cc $(INCLUDES) -L $(ERPC_DIR)/build $(LIBS) -DDPDK=true -march=native -DCOMMON_DIR=$(shell cd ..; pwd) -DLOGGER_DIR=$(shell cd ..; pwd)

clean:
	rm proxy

