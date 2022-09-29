LIBS = -lerpc -lpthread -lnuma -ldl -ldpdk -lgflags
ERPC_DIR = /proj/sequencer/eRPC
INCLUDES = -I $(ERPC_DIR)/src -I /usr/include/dpdk

sequencer: sequencer.cc sequencer.h recovery.h ../common.h ../bitmap.h
	bash ../ltomake sequencer

slowsequencer: sequencer.cc sequencer.h recovery.h ../common.h
	g++ -O3 -g -rdynamic -std=c++11 -o sequencer sequencer.cc $(INCLUDES) -L $(ERPC_DIR)/build $(LIBS) -DDPDK=true -DCOMMON_DIR=$(shell cd ../; pwd) -march=native

clean:
	rm sequencer
