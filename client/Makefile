LIBS = -lerpc -lpthread -lnuma -ldl -ldpdk -lgflags
ERPC_DIR = /proj/sequencer/eRPC
INCLUDES = -I $(ERPC_DIR)/src -I /usr/include/dpdk -I $(ERPC_DIR)

client: client.cc client.h ../common.h
	bash ../ltomake client

slowclient: client.cc client.h ../common.h
	g++ -O3 -g -rdynamic -std=c++11 -o client client.cc $(INCLUDES) -L $(ERPC_DIR)/build $(LIBS) -DDPDK=true -DCOMMON_DIR=$(shell cd ../; pwd) -march=native

clean:
	rm client
