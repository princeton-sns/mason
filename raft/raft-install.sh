make clean
#sudo rm -fr /usr/local/include/raft/
#sudo rm /usr/local/lib/libraft.a
make CFLAGS='-Iinclude -Werror -Werror=return-type -Werror=uninitialized \
  -Wcast-align -Wno-pointer-sign -fno-omit-frame-pointer -fno-common \
  -fsigned-char -I CLinkedListQueue/ -g -O4 -DNDEBUG -fPIC -march=native'

sudo mkdir -p /usr/local/include/raft
sudo cp include/* /usr/local/include/raft/
sudo cp libraft.a /usr/local/lib
sudo cp libraft.so /usr/local/lib

sudo ldconfig
