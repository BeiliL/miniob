mkdir -p build && cd build && cmake .. -DDEBUG=ON -DCMAKE_C_COMPILER=/opt/rh/devtoolset-11/root/bin/gcc -DCMAKE_CXX_COMPILER=/opt/rh/devtoolset-11/root/bin/g++ && make -j4
