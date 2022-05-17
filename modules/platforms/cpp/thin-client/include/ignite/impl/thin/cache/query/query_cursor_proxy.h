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

#ifndef _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_CURSOR_PROXY
#define _IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_CURSOR_PROXY

#include <vector>

#include <ignite/common/concurrent.h>
#include <ignite/ignite_error.h>

#include <ignite/thin/cache/cache_entry.h>

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
                     * Query cursor class implementation.
                     */
                    class QueryCursorProxy
                    {
                    public:
                        /**
                         * Default constructor.
                         */
                        QueryCursorProxy()
                        {
                            // No-op.
                        }

                        /**
                         * Check whether next entry exists.
                         *
                         * @return True if next entry exists.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        bool HasNext();

                        /**
                         * Get next entry.
                         *
                         * @param entry Entry.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        void GetNext(Readable& entry);

                        /**
                         * Get all entries.
                         *
                         * @param collection Output collection.
                         *
                         * @throw IgniteError class instance in case of failure.
                         */
                        void GetAll(Readable& collection);

                    private:
                        /** Implementation delegate. */
                    ignite::common::concurrent::SharedPointer<void> impl;
                    };
                }
            }
        }
    }
}

#endif //_IGNITE_IMPL_THIN_CACHE_QUERY_QUERY_CURSOR_PROXY
