#! /bin/bash
if [[ $* == *--eRPC* ]]; then
    rm eRPC/CMakeCache.txt
    rm eRPC/CMakeFiles -fr
fi
if [[ $* == *--clean* ]]; then
cd proxy; make clean; cd ..
cd client; make clean; cd ..
cd sequencer; make clean; cd ..
cd server; make clean; cd ..
fi
if [[ $* == *--touch* ]]; then
cd proxy; touch proxy.cc; cd ..
cd client; touch client.cc; cd ..
cd sequencer; touch sequencer.cc; cd ..
cd server; touch server.cc; cd ..
fi
cd proxy; make; cd ..
cd client; make; cd ..
cd sequencer; make; cd ..
cd server; make; cd ..