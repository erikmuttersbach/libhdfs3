/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#ifndef _HDFS_LIBHDFS3_RPC_SASLAUTH_H_
#define _HDFS_LIBHDFS3_RPC_SASLAUTH_H_

#include "rpc/RpcAuth.h"
#include "Hash.h"

#include <string>

namespace Hdfs {
namespace Internal {

class SaslAuth {
public:
    explicit SaslAuth(const RpcAuth & auth) :
        rpcAuth(auth) {
    }

    std::string getMechanism() const {
        return mechanism;
    }

    void setMechanism(std::string mechanism) {
        this->mechanism = mechanism;
    }

    const RpcAuth & getRpcAuth() const {
        return rpcAuth;
    }

    void setRpcAuth(const RpcAuth & rpcAuth) {
        this->rpcAuth = rpcAuth;
    }

    std::string getServerHost() const {
        return serverHost;
    }

    void setServerHost(std::string serverHost) {
        this->serverHost = serverHost;
    }

    std::string getService() const {
        return service;
    }

    void setService(std::string service) {
        this->service = service;
    }

    size_t hash_value() const;

    bool operator ==(const SaslAuth & other) const {
        return rpcAuth == other.rpcAuth
               && mechanism == other.mechanism && service == other.service
               && serverHost == other.serverHost;
    }

private:
    RpcAuth rpcAuth;
    std::string mechanism;
    std::string serverHost;
    std::string service;
};

}
}

HDFS_HASH_DEFINE(::Hdfs::Internal::SaslAuth);

#endif /* _HDFS_LIBHDFS3_RPC_SASLAUTH_H_ */
