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

#ifndef _IGNITE_NETWORK_ASYNC_CLIENT_POOL
#define _IGNITE_NETWORK_ASYNC_CLIENT_POOL

#include <stdint.h>

#include <vector>

#include <ignite/ignite_error.h>
#include <ignite/impl/interop/interop_memory.h>

#include <ignite/network/async_handler.h>
#include <ignite/network/tcp_range.h>

namespace ignite
{
    namespace network
    {
        class AsyncClientPool
        {
        public:
            /**
             * Destructor.
             */
            virtual ~AsyncClientPool()
            {
                // No-op.
            }

            /**
             * Start internal thread that establishes connections to provided addresses and asynchronously sends and
             * receives messages from them. Function returns either when thread is started and first connection is
             * established or failure happened.
             *
             * @param addrs Addresses to connect to.
             * @param handler Async event handler.
             * @param timeout Connection establishment timeout.
             * @param connLimit Connection upper limit. Zero means limit is disabled.
             * @throw IgniteError on error.
             */
            virtual void Start(const std::vector<TcpRange>& addrs, AsyncHandler& handler, uint32_t connLimit) = 0;

            /**
             * Close all established connections and stops handling threads.
             */
            virtual void Stop() = 0;

            /**
             * Send data to specific established connection.
             *
             * @param id Client ID.
             * @param mem Data to be sent.
             * @return @c true if connection is present and @c false otherwise.
             * @throw IgniteError on error.
             */
            virtual bool Send(uint64_t id, impl::interop::SP_InteropMemory mem) = 0;

            /**
             * Closes specified connection if it's established. Connection to the specified address is planned for
             * re-connect. Error is reported to handler.
             *
             * @param id Client ID.
             */
            virtual void Close(uint64_t id, const IgniteError* err) = 0;
        };

        // Type alias
        typedef common::concurrent::SharedPointer<network::AsyncClientPool> SP_AsyncClientPool;
    }
}

#endif //_IGNITE_NETWORK_ASYNC_CLIENT_POOL