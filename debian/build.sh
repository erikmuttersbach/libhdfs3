#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

this_dir=`cd "\`dirname \"$0\"\`";pwd`
top_dir=${this_dir}/../

die() {
    echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_depends() {
    apt-get update || die "cannot update repository"
    apt-get install -qq \
        software-properties-common \
        dpkg-dev debhelper g++ cmake libxml2-dev uuid-dev protobuf-compiler \
        libgtest-dev libkrb5-dev libgsasl7-dev \
        libprotobuf-dev libgsasl7-dev libkrb5-dev libboost-all-dev || die "cannot install dependencies"
}

build_with_boost() {
    pushd ${top_dir}
    rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
    ../bootstrap --enable-boost || die "bootstrap failed"
    make -j2 || die "failed to run unit tests"
    popd
}

build_with_debug() {
    pushd ${top_dir}
    rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
    ../bootstrap --enable-debug || die "bootstrap failed"
    make -j2 || die "failed to run unit tests"
    popd
}

unit_tests() {
    pushd ${top_dir}
    rm -rf build && mkdir -p build && cd build || die "cannot create build directory"
    ../bootstrap --enable-debug || die "bootstrap failed"
    make -j2 unittest || die "failed to run unit tests"
    popd
}

"$@"
