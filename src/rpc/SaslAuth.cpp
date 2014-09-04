/********************************************************************
 * Copyright (c) 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
#include "SaslAuth.h"

namespace Hdfs {
namespace Internal {

size_t SaslAuth::hash_value() const {
    size_t values[] = { rpcAuth.hash_value(), StringHasher(mechanism),
                        StringHasher(serverHost), StringHasher(service)
                      };
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
}

}
}
