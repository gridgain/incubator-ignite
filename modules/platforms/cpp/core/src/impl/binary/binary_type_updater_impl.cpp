/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <iterator>

#include "ignite/impl/binary/binary_type_updater_impl.h"
#include "ignite/impl/interop/interop_output_stream.h"
#include "ignite/impl/binary/binary_writer_impl.h"
#include "ignite/binary/binary_raw_writer.h"
#include "ignite/binary/binary_raw_reader.h"

using namespace ignite::common::concurrent;
using namespace ignite::jni::java;
using namespace ignite::java;
using namespace ignite::impl;
using namespace ignite::impl::interop;
using namespace ignite::binary;

namespace ignite
{    
    namespace impl
    {
        namespace binary
        {
            enum Operation
            {
                /** Operation: metadata get. */
                OP_GET_META = 1,

                /** Operation: metadata update. */
                OP_PUT_META = 3
            };

            BinaryTypeUpdaterImpl::BinaryTypeUpdaterImpl(IgniteEnvironment& env, jobject javaRef) :
                env(env),
                javaRef(javaRef)
            {
                // No-op.
            }

            BinaryTypeUpdaterImpl::~BinaryTypeUpdaterImpl()
            {
                JniContext::Release(javaRef);
            }

            bool BinaryTypeUpdaterImpl::Update(const Snap& snap, IgniteError& err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> mem = env.AllocateMemory();

                InteropOutputStream out(mem.Get());
                BinaryWriterImpl writer(&out, 0);
                BinaryRawWriter rawWriter(&writer);

                // We always pass only one meta at a time in current implementation for simplicity.
                rawWriter.WriteInt32(1);

                rawWriter.WriteInt32(snap.GetTypeId());
                rawWriter.WriteString(snap.GetTypeName());
                rawWriter.WriteString(0); // Affinity key is not supported for now.
                
                if (snap.HasFields())
                {
                    const Snap::FieldTypeMap& fields = snap.GetFieldTypeMap();

                    rawWriter.WriteInt32(static_cast<int32_t>(fields.size()));

                    for (Snap::FieldTypeMap::const_iterator it = fields.begin(); it != fields.end(); ++it)
                    {
                        rawWriter.WriteString(it->first);
                        rawWriter.WriteInt32(it->second);
                    }
                }
                else
                    rawWriter.WriteInt32(0);

                rawWriter.WriteBool(false); // Enums are not supported for now.

                rawWriter.WriteInt32(0); // Schema size. Compact schema footer is not yet supported.

                out.Synchronize();

                long long res = env.Context()->TargetInStreamOutLong(javaRef, OP_PUT_META, mem.Get()->PointerLong(), &jniErr);

                IgniteError::SetError(jniErr.code, jniErr.errCls, jniErr.errMsg, err);

                return jniErr.code == IGNITE_JNI_ERR_SUCCESS && res == 1;
            }

            SPSnap BinaryTypeUpdaterImpl::GetMeta(int32_t typeId, IgniteError& err)
            {
                JniErrorInfo jniErr;

                SharedPointer<InteropMemory> outMem = env.AllocateMemory();
                SharedPointer<InteropMemory> inMem = env.AllocateMemory();

                InteropOutputStream out(outMem.Get());
                BinaryWriterImpl writer(&out, 0);

                writer.WriteInt32(typeId);

                env.Context()->TargetInStreamOutStream(javaRef, OP_GET_META,
                    outMem.Get()->PointerLong(), inMem.Get()->PointerLong(), &jniErr);

                InteropInputStream in(inMem.Get());
                BinaryReaderImpl reader(&in);
                BinaryRawReader rawReader(&reader);

                bool found = rawReader.ReadBool();

                if (!found)
                    return SPSnap();

                int32_t readTypeId = rawReader.ReadInt32();

                assert(typeId == readTypeId);

                std::string typeName = rawReader.ReadString();

                SPSnap res(new Snap(typeName, readTypeId));

                // Skipping affinity key field name.
                rawReader.ReadString();

                BinaryMapReader<std::string, int32_t> mapReader = rawReader.ReadMap<std::string, int32_t>();

                while (mapReader.HasNext())
                {
                    std::string fieldName;
                    int32_t fieldId;

                    mapReader.GetNext(&fieldName, &fieldId);

                    std::cout << "fieldName: " << fieldName << ", fieldId: " << fieldId << std::endl;

                    res.Get()->AddField(fieldId, fieldName, 0);
                }

                // Skipping isEnum info.
                rawReader.ReadBool();

                return res;
            }
        }
    }
}