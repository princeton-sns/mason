#! /bin/bash
if [[ $* == *--eRPC* ]]; then
    rm eRPC/CMakeCache.txt
    rm eRPC/CMakeFiles -fr
fi
cd proxy; make; cd ..
cd client; make; cd ..
cd sequencer; make; cd ..
cd server; make; cd ..