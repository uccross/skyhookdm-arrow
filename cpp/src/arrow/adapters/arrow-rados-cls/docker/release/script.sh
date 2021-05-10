#!/bin/bash
set -eux

cd /arrow/cpp

mkdir -p debug
cd debug
cmake   -DCMAKE_BUILD_TYPE=Debug \
        -DARROW_CLS=ON \
        -DARROW_PARQUET=ON \
        -DARROW_WITH_SNAPPY=ON \
        -DARROW_WITH_ZLIB=ON \
        -DARROW_DATASET=ON \
        -DARROW_PYTHON=ON \
        -DARROW_CSV=ON \
        -DARROW_WITH_LZ4=ON \
        -DARROW_WITH_ZSTD=ON \
        ..

make -j4 install

cp ./debug/libcls_arrow* /usr/lib64/rados-classes/
cp -r /usr/local/lib64/. /usr/lib64

export PYARROW_BUILD_TYPE=Debug
export PYARROW_WITH_DATASET=1
export PYARROW_WITH_PARQUET=1
export PYARROW_WITH_RADOS=1

cd /arrow/python

pip3 install -r requirements-build.txt -r requirements-test.txt
pip3 install wheel
python3 setup.py build_ext --inplace --bundle-arrow-cpp bdist_wheel
pip3 install --upgrade dist/*.whl
cp -r dist/*.whl /

python3 -c "import pyarrow"
python3 -c "import pyarrow.dataset"
python3 -c "import pyarrow.parquet"
