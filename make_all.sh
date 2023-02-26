#! /bin/bash
if [[ $* == *--eRPC* ]]; then
    rm eRPC/CMakeCache.txt
    rm eRPC/CMakeFiles -fr
fi
if [[ $* == *--clean* ]]; then
    cd proxy; make clean; make; cd ..
    cd client; make clean; make; cd ..
fi
cd proxy; make; cd ..
cd client; make; cd ..