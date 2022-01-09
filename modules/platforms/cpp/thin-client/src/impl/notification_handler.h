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

#ifndef _IGNITE_IMPL_THIN_NOTIFICATION_HANDLER
#define _IGNITE_IMPL_THIN_NOTIFICATION_HANDLER

#include <stdint.h>

#include <vector>

#include <ignite/ignite_error.h>
#include <ignite/network/data_buffer.h>

#include <ignite/impl/interop/interop_memory.h>

namespace ignite
{
    namespace impl
    {
        namespace thin
        {
            /** Notification handler. */
            class NotificationHandler
            {
            public:
                /**
                 * Destructor.
                 */
                virtual ~NotificationHandler()
                {
                    // No-op.
                }

                /**
                 * Handle notification.
                 *
                 * @param msg Message.
                 * @return @c true if processing complete.
                 */
                virtual bool OnNotification(const network::DataBuffer& msg) = 0;
            };

            /** Shared pointer to notification handler. */
            typedef common::concurrent::SharedPointer<NotificationHandler> SP_NotificationHandler;

            /** Notification handler. */
            class NotificationHandlerHolder
            {
                /** Message queue. */
                typedef std::vector<network::DataBuffer> MessageQueue;

            public:
                /**
                 * Default constructor.
                 */
                NotificationHandlerHolder() :
                    queue(),
                    handler(),
                    complete(false)
                {
                    // No-op.
                }

                /**
                 * Destructor.
                 */
                ~NotificationHandlerHolder()
                {
                    // No-op.
                }

                /**
                 * Process notification.
                 *
                 * @param msg Notification message to process.
                 */
                void ProcessNotification(const network::DataBuffer& msg)
                {
                    std::cout << "=============== 0000000000000000  ProcessNotification: this=" << this << std::endl;
                    std::cout << "=============== 0000000000000000  ProcessNotification: complete=" << (complete ? "TRUE" : "FALSE") << std::endl;
                    if (complete)
                        return;

                    if (handler.IsValid())
                    {
                        std::cout << "=============== 0000000000000000  ProcessNotification: handler.IsValid()" << std::endl;
                        std::cout << "=============== 0000000000000000  ProcessNotification: handler=" << handler.Get() << std::endl;

                        complete = handler.Get()->OnNotification(msg);
                        std::cout << "=============== 0000000000000000  ProcessNotification: complete=" << (complete ? "TRUE" : "FALSE") << std::endl;
                    }
                    else
                    {
                        std::cout << "=============== 0000000000000000  ProcessNotification: !handler.IsValid()" << std::endl;

                        queue.push_back(msg.Clone());

                        std::cout << "=============== 0000000000000000  ProcessNotification: queue.size=" << queue.size() << std::endl;
                    }
                }

                /**
                 * Set handler.
                 *
                 * @param handler Notification handler.
                 */
                void SetHandler(const SP_NotificationHandler& handler)
                {
                    std::cout << "=============== 0000000000000000  SetHandler: handler=" << &handler << std::endl;
                    if (this->handler.IsValid())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            "Internal error: handler is already set for the notification");

                    this->handler = handler;
                    std::cout << "=============== 0000000000000000  SetHandler: this->handler=" << this->handler.Get() << std::endl;

                    for (MessageQueue::iterator it = queue.begin(); it != queue.end(); ++it)
                    {
                        std::cout << "=============== 0000000000000000  SetHandler: complete=" << (complete ? "TRUE" : "FALSE") << std::endl;
                        complete = complete || this->handler.Get()->OnNotification(*it);
                    }

                    queue.clear();
                }

                /**
                 * Check whether processing complete.
                 *
                 * @return @c true if processing complete.
                 */
                bool IsProcessingComplete() const
                {
                    return complete;
                }

            private:
                /** Notification queue. */
                MessageQueue queue;

                /** Notification handler. */
                SP_NotificationHandler handler;

                /** Processing complete. */
                bool complete;
            };
        }
    }
}

#endif //_IGNITE_IMPL_THIN_NOTIFICATION_HANDLER
