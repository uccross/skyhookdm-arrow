ARG CEPH_VERSION=v15.2.4

FROM ceph/ceph:$CEPH_VERSION

RUN dnf group install -y "Development Tools" && \
    dnf install -y cmake \
                   wget \
                   curl \
                   rados-objclass-devel \
                   python3-rados \
                   librados-devel && \
    dnf clean all

WORKDIR /

COPY . /arrow

RUN /arrow/ci/scripts/skyhook_build.sh 

RUN rm -rf /arrow
