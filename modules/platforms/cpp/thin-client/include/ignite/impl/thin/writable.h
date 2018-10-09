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

#ifndef _IGNITE_IMPL_THIN_WRITABLE
#define _IGNITE_IMPL_THIN_WRITABLE

#include <ignite/binary/binary_raw_writer.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /**
             * Abstraction to any type that can be written to a binary stream.
             */
            class Writable
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~Writable()
                {
                    // No-op.
                }

                /**
                 * Write value using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const = 0;
            };

            /**
             * Implementation of the Writable class for a concrete type.
             */
            template<typename T>
            class WritableImpl : public Writable
            {
            public:
                /** Value type. */
                typedef T ValueType;

                /**
                 * Constructor.
                 *
                 * @param value Value.
                 */
                WritableImpl(const ValueType& value) :
                    value(value)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableImpl()
                {
                    // No-op.
                }

                /**
                 * Write value using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const
                {
                    writer.WriteObject(value);
                }

            private:
                /** Value. */
                const ValueType& value;
            };

            /**
             * Implementation of the Writable class for a sequence.
             */
            template<typename T, typename I>
            class WritableSequenceImpl : public Writable
            {
            public:
                /** Iterator type. */
                typedef T ElementType;

                /** Iterator type. */
                typedef I IteratorType;

                /**
                 * Constructor.
                 *
                 * @param begin Begin of the sequence.
                 * @param end Sequence end.
                 */
                WritableSequenceImpl(IteratorType begin, IteratorType end) :
                    begin(begin),
                    end(end)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                virtual ~WritableSequenceImpl()
                {
                    // No-op.
                }

                /**
                 * Write sequence using writer.
                 *
                 * @param writer Writer to use.
                 */
                virtual void Write(binary::BinaryWriterImpl& writer) const
                {
                    using namespace ignite::binary;

                    interop::InteropOutputStream* out = writer.GetStream();

                    int32_t cntPos = out->Reserve(4);

                    out->Synchronize();

                    int32_t cnt = 0;
                    for (IteratorType it = begin; it != end; ++it)
                    {
                        writer.WriteObject(*it);

                        ++cnt;
                    }

                    out->WriteInt32(cntPos, cnt);

                    out->Synchronize();
                }

            private:
                /** Sequence begin. */
                IteratorType begin;

                /** Sequence end. */
                IteratorType end;
            };
        }
    }
}

#endif // _IGNITE_IMPL_THIN_WRITABLE
