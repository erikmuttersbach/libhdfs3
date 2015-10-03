#!/bin/bash

die() {
    echo "$@" 1>&2 ; popd 2>/dev/null; exit 1
}

install_depends() {
    apt-get install -qq software-properties-common || die "cannot install software-properties-common"
    add-apt-repository -y ppa:ubuntu-toolchain-r/test || die "cannot add repository"
    apt-get update || die "cannot update repository"
    apt-get install -qq \
        dpkg-dev g++ cmake libxml2-dev uuid-dev protobuf-compiler \
        libprotobuf-dev libgsasl7-dev libkrb5-dev libboost-all-dev || die "cannot install dependencies"
}

build_with_boost() {
    rm -rf build
    mkdir -p build || die "cannot create build directory"
    pushd build || die "cannot enter build directory"
    ../bootstrap --enable-boost || die "bootstrap failed"
    make || die "build failed"
    make unittest || die "failed to run unit tests"
    popd
}

build_with_debug() {
    rm -rf build
    mkdir -p build || die "cannot create build directory"
    pushd build || die "cannot enter build directory"
    ../bootstrap --enable-debug || die "bootstrap failed"
    make || die "build failed"
    make unittest || die "failed to run unit tests"
    popd
}

create_package() {
	rm -rf build
	mkdir -p build || die "cannot create build directory"
    pushd build || die "cannot enter build directory"
    ../bootstrap || die "bootstrap failed"
	make debian-package || die "failed to create debian package"
	popd
}

run() {
    install_depends || die "failed to install dependencies"
    build_with_boost || die "build failed with boost"
    build_with_debug || die "build failed with debug mode"
    create_package || die "failed to create debian package"
}

"$@"
