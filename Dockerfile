FROM ubuntu:18.04

ADD . /libhdfs3

RUN cd libhdfs3/debian && \
    ./build.sh install_depends && \
    ./build.sh build_with_debug