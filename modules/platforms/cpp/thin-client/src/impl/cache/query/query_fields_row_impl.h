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

/**
 * @file
 * Declares ignite::impl::thin::cache::query::QueryFieldsRow class.
 */

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_FIELDS_ROW_IMPL
#define _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_FIELDS_ROW_IMPL

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include <ignite/impl/binary/binary_reader_impl.h>

#include <ignite/impl/thin/readable.h>

#include "impl/cache/query/cursor_page.h"

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            namespace cache
            {
                namespace query
                {
                    /**
                     * Query fields row implementation.
                     */
                    class QueryFieldsRowImpl
                    {
                    public:
                        /**
                         * Constructor.
                         *
                         * @param page Cursor page.
                         * @param posInMem Row starting position in memory.
                         */
                        QueryFieldsRowImpl(const SP_CursorPage& page, int32_t posInMem) :
                            size(0),
                            pos(0),
                            page(page),
                            stream(this->page.Get()->GetMemory()),
                            reader(&stream)
                        {
                            stream.Position(posInMem);
                            size = reader.ReadInt32();
                        }

                        /**
                         * Check whether next entry exists.
                         *
                         * @return True if next entry exists.
                         */
                        bool HasNext() const
                        {
                            return pos < size;
                        }

                        /**
                         * Get next entry.
                         *
                         * @param readable Value to read.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        void GetNext(Readable& readable)
                        {
                            readable.Read(reader);
                            ++pos;
                        }

                        /**
                         * Get next entry assuming it's an array of 8-byte signed integers. Maps to "byte[]" type in Java
                         * and BINARY type in SQL.
                         *
                         * @param dst Array to store data to.
                         * @param len Expected length of array.
                         * @return Actual amount of elements read. If "len" argument is less than actual array size or
                         *     resulting array is set to null, nothing will be written to resulting array and returned value
                         *     will contain required array length.
                         *     @c -1 will be returned in case array in stream was null.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        int32_t GetNextInt8Array(int8_t *dst, int32_t len)
                        {
                            int32_t res = reader.ReadInt8Array(dst, len);

                            if (res <= len)
                                ++pos;

                            return res;
                        }

                        /**
                         * Get size of the row in elements.
                         *
                         * @return
                         */
                        int32_t GetSize() const
                        {
                            return size;
                        }

                    private:
                        /** Number of elements in row. */
                        int32_t size;

                        /** Current position in row. */
                        int32_t pos;

                        /** Cursor page. */
                        SP_CursorPage page;

                        /** Stream. */
                        interop::InteropInputStream stream;

                        /** Reader. */
                        binary::BinaryReaderImpl reader;
                    };

                    /** Query field row implementation shared pointer. */
                    typedef common::concurrent::SharedPointer<QueryFieldsRowImpl> SP_QueryFieldsRowImpl;
                }
            }
        }
    }    
}

#endif //_IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_FIELDS_ROW_IMPL
