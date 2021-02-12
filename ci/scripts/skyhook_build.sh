#!/bin/bash
set -eux

# c++ api and cls build
cd /arrow/cpp

mkdir -p debug
cd debug
cmake -DCMAKE_BUILD_TYPE=Debug \
          -DARROW_CLS=ON \
          -DARROW_PARQUET=ON \
          -DARROW_WITH_SNAPPY=ON \
          -DARROW_WITH_ZLIB=ON \
          -DARROW_DATASET=ON \
          -DARROW_PYTHON=ON \
          -DARROW_CSV=ON \
          ..

make -j4 install

cp ./debug/libcls_arrow* /usr/lib64/rados-classes/
cp -r /usr/local/lib64/. /usr/lib64

# python api build
cd /arrow/python

pip3 install -r requirements-build.txt -r requirements-test.txt
pip3 install wheel

export WORKDIR=${WORKDIR:-$HOME}
export ARROW_HOME=$WORKDIR/dist
export LD_LIBRARY_PATH=$ARROW_HOME/lib
export PYARROW_BUILD_TYPE=Debug
export PYARROW_WITH_DATASET=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_RADOS=1

mkdir -p /root/dist/lib
mkdir -p /root/dist/include

cp -r /usr/local/lib64/. /root/dist/lib
cp -r /usr/local/include/. /root/dist/include

python3 setup.py build_ext --inplace --bundle-arrow-cpp bdist_wheel
pip3 install dist/*.whl
cp -r dist/*.whl /

# check installation
python3 -c "import pyarrow"
python3 -c "import pyarrow.dataset"
python3 -c "import pyarrow.parquet"
