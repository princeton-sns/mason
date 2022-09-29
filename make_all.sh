#! /bin/bash
# include --eRPC to rebuild eRPC or --clean to rebuild all components
if [[ $* == *--eRPC* ]]; then
    rm eRPC/CMakeCache.txt
    rm eRPC/CMakeFiles -fr
fi
if [[ $* == *--clean* ]]; then
    cd proxy; make clean; make; cd ..
    cd client; make clean; make; cd ..
    cd sequencer; make clean; make; cd ..
else
    cd proxy; make; cd ..
    cd client; make; cd ..
    cd sequencer; make; cd ..
fi