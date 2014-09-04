/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 - 
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "Memory.h"
#include "RpcCall.h"
#include "RpcContentWrapper.h"
#include "RpcRemoteCall.h"
#include "WriteBuffer.h"

#include <google/protobuf/io/coded_stream.h>

using namespace google::protobuf::io;

namespace Hdfs {
namespace Internal {

void RpcRemoteCall::serialize(const RpcProtocolInfo & protocol,
                              WriteBuffer & buffer) {
    std::vector<char> req;
    req.resize(call.getRequest()->ByteSize());
    call.getRequest()->SerializeToArray(&req[0], req.size());
    HadoopRpcRequestProto reqProto;
    reqProto.set_methodname(call.getName());
    reqProto.set_declaringclassprotocolname(protocol.getProtocol().c_str());
    reqProto.set_clientprotocolversion(protocol.getVersion());
    reqProto.set_request(&req[0], req.size());
    RpcPayloadHeaderProto header;
    header.set_callid(identity);
    header.set_rpckind(RpcKindProto::RPC_PROTOCOL_BUFFER);
    header.set_rpcop(RpcPayloadOperationProto::RPC_FINAL_PAYLOAD);
    int headerSize = header.ByteSize();
    int32_t codedHeaderSize = CodedOutputStream::VarintSize32(headerSize);
    int32_t payloadSize = reqProto.ByteSize();
    int32_t codedPayloadSize = CodedOutputStream::VarintSize32(payloadSize);
    int32_t total = codedHeaderSize + headerSize + codedPayloadSize
                    + payloadSize;
    buffer.writeBigEndian(total);
    CodedOutputStream::WriteVarint32SignExtendedToArray(headerSize,
            reinterpret_cast<unsigned char *>(buffer.alloc(codedHeaderSize)));
    header.SerializeWithCachedSizesToArray(
        reinterpret_cast<unsigned char *>(buffer.alloc(headerSize)));
    CodedOutputStream::WriteVarint32SignExtendedToArray(payloadSize,
            reinterpret_cast<unsigned char *>(buffer.alloc(codedPayloadSize)));
    reqProto.SerializeWithCachedSizesToArray(
        reinterpret_cast<unsigned char *>(buffer.alloc(payloadSize)));
}

}
}

