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

#include <algorithm>

#include <ignite/network/utils.h>

#include "network/sockets.h"
#include "network/win_async_client_pool.h"

namespace ignite
{
    namespace network
    {
        WinAsyncClientPool::ConnectingThread::ConnectingThread(WinAsyncClientPool& clientPool) :
            clientPool(clientPool),
            stopping(false),
            connectNeeded()
        {
            // No-op.
        }

        void WinAsyncClientPool::ConnectingThread::Run()
        {
            while (!stopping)
            {
                TcpRange range;

                {
                    common::concurrent::CsLockGuard lock(clientPool.clientsCs);

                    while (!clientPool.isConnectionNeededLocked())
                    {
                        connectNeeded.Wait(clientPool.clientsCs);

                        if (stopping)
                            return;
                    }

                    size_t idx = rand() % clientPool.nonConnected.size();
                    range = clientPool.nonConnected.at(idx);
                }

                SP_WinAsyncClient client = TryConnect(range);

                if (!client.IsValid())
                    continue;

                if (stopping)
                {
                    client.Get()->Close();

                    return;
                }

                uint64_t id = 0;

                try
                {
                    id = clientPool.AddClient(client);
                }
                catch (const IgniteError& err)
                {
                    client.Get()->Close();

                    if (clientPool.asyncHandler)
                        clientPool.asyncHandler->OnConnectionError(client.Get()->GetAddress(), err);

                    continue;
                }

                if (clientPool.asyncHandler)
                    clientPool.asyncHandler->OnConnectionSuccess(client.Get()->GetAddress(), id);

                PostQueuedCompletionStatus(clientPool.iocp, 0, reinterpret_cast<ULONG_PTR>(client.Get()), 0);
            }
        }

        void WinAsyncClientPool::ConnectingThread::WakeUp()
        {
            connectNeeded.NotifyOne();
        }

        void WinAsyncClientPool::ConnectingThread::Stop()
        {
            stopping = true;

            WakeUp();

            Join();
        }

        SP_WinAsyncClient WinAsyncClientPool::ConnectingThread::TryConnect(const TcpRange& range)
        {
            for (uint16_t port = range.port; port <= (range.port + range.range); ++port)
            {
//                std::cout << "=============== ConnectingThread: port=" << port << std::endl;
//                std::cout << "=============== ConnectingThread: range.port=" << range.port << std::endl;
//                std::cout << "=============== ConnectingThread: range.range=" << range.range << std::endl;

                EndPoint addr(range.host, port);
                try
                {
                    SP_WinAsyncClient client = TryConnect(addr);

                    if (!client.IsValid())
                        continue;

                    client.Get()->SetRange(range);

                    return client;
                }
                catch (const IgniteError& err)
                {
                    if (clientPool.asyncHandler)
                        clientPool.asyncHandler->OnConnectionError(addr, err);
                }
            }

            return SP_WinAsyncClient();
        }

        SP_WinAsyncClient WinAsyncClientPool::ConnectingThread::TryConnect(const EndPoint& addr)
        {
            addrinfo hints;
            memset(&hints, 0, sizeof(hints));

            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = SOCK_STREAM;
            hints.ai_protocol = IPPROTO_TCP;

            std::stringstream converter;
            converter << addr.port;
            std::string strPort = converter.str();

            // Resolve the server address and port
            addrinfo *result = NULL;
            int res = getaddrinfo(addr.host.c_str(), strPort.c_str(), &hints, &result);

            if (res != 0)
                utils::ThrowNetworkError("Can not resolve host: " + addr.host + ":" + strPort);

            std::string lastErrorMsg = "Failed to resolve host";

            SOCKET socket = INVALID_SOCKET;

            // Attempt to connect to an address until one succeeds
            for (addrinfo *it = result; it != NULL; it = it->ai_next)
            {
                lastErrorMsg = "Failed to establish connection with the host";

                socket = WSASocket(it->ai_family, it->ai_socktype, it->ai_protocol, NULL, 0, WSA_FLAG_OVERLAPPED);

                if (socket == INVALID_SOCKET)
                    utils::ThrowNetworkError("Socket creation failed: " + sockets::GetLastSocketErrorMessage());

                enum { BUFFER_SIZE = 0x10000 };
                sockets::TrySetSocketOptions(socket, BUFFER_SIZE, TRUE, TRUE, TRUE);

                // Connect to server.
                res = WSAConnect(socket, it->ai_addr, static_cast<int>(it->ai_addrlen), NULL, NULL, NULL, NULL);
                if (SOCKET_ERROR == res)
                {
                    closesocket(socket);
                    socket = INVALID_SOCKET;

                    int lastError = WSAGetLastError();

                    if (lastError != WSAEWOULDBLOCK)
                    {
                        lastErrorMsg.append(": ").append(sockets::GetSocketErrorMessage(lastError));

                        continue;
                    }
                }

                break;
            }

            freeaddrinfo(result);

            if (socket == INVALID_SOCKET)
                utils::ThrowNetworkError(lastErrorMsg);

            return SP_WinAsyncClient(new WinAsyncClient(socket, addr));
        }

        WinAsyncClientPool::WorkerThread::WorkerThread(WinAsyncClientPool& clientPool) :
            clientPool(clientPool),
            stopping(false)
        {
            // No-op.
        }

        void WinAsyncClientPool::WorkerThread::Run()
        {
            while (!stopping)
            {
                DWORD bytesTransferred = 0;
                ULONG_PTR key = NULL;
                LPOVERLAPPED overlapped = NULL;

                BOOL ok = GetQueuedCompletionStatus(clientPool.iocp, &bytesTransferred, &key, &overlapped, INFINITE);

//                std::cout << "=============== WorkerThread: Got event" << std::endl;

                if (stopping)
                    break;

                if (!key)
                    continue;

                WinAsyncClient* client = reinterpret_cast<WinAsyncClient*>(key);

                if (!ok || (0 != overlapped && 0 == bytesTransferred))
                {
//                    std::cout << "=============== WorkerThread: closing " << client->GetId() << std::endl;
//                    std::cout << "=============== WorkerThread: bytesTransferred " << bytesTransferred << std::endl;

                    IgniteError err(IgniteError::IGNITE_ERR_NETWORK_FAILURE, "Connection closed");
                    clientPool.CloseAndRelease(client->GetId(), &err);

                    continue;
                }

                if (!overlapped)
                {
                    // This mean new client is connected.
//                    std::cout << "=============== WorkerThread: New connection. Initiating recv " << client->GetId() << std::endl;
                    bool success = client->ReceiveAll(IoOperation::PACKET_HEADER_SIZE);
                    if (!success)
                    {
                        IgniteError err(IgniteError::IGNITE_ERR_GENERIC, "Can not initiate receiving of a first packet");

                        clientPool.Close(client->GetId(), &err);
                    }

                    continue;
                }

                try
                {
                    IoOperation* operation = reinterpret_cast<IoOperation*>(overlapped);
                    switch (operation->kind)
                    {
                        case IoOperationKind::SEND:
                        {
//                            std::cout << "=============== WorkerThread: processing send " << bytesTransferred << std::endl;
                            client->ProcessSent(bytesTransferred);

                            break;
                        }

                        case IoOperationKind::RECEIVE:
                        {
//                            std::cout << "=============== WorkerThread: processing recv " << bytesTransferred << std::endl;
                            impl::interop::SP_InteropMemory packet = client->ProcessReceived(bytesTransferred);

                            if (packet.IsValid() && clientPool.asyncHandler)
                                clientPool.asyncHandler->OnMessageReceived(client->GetId(), packet);

                            break;
                        }

                        default:
                            break;
                    }
                }
                catch (const IgniteError& err)
                {
                    clientPool.Close(client->GetId(), &err);
                }
            }
        }

        void WinAsyncClientPool::WorkerThread::Stop()
        {
            stopping = true;

            PostQueuedCompletionStatus(clientPool.iocp, 0, 0, 0);

            Join();
        }

        WinAsyncClientPool::WinAsyncClientPool() :
            stopping(false),
            asyncHandler(0),
            connectingThread(*this),
            workerThread(*this),
            connectionLimit(0),
            idGen(0),
            iocp(NULL),
            clientsCs(),
            nonConnected(),
            clientIdMap(),
            clientAddrMap()
        {
            // No-op.
        }

        WinAsyncClientPool::~WinAsyncClientPool()
        {
            InternalStop();
        }

        void WinAsyncClientPool::Start(const std::vector<TcpRange>& addrs, AsyncHandler& handler, uint32_t connLimit)
        {
            stopping = false;

            if (asyncHandler)
                throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Client pool is already started");

            sockets::InitWsa();

            iocp = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
            if (iocp == NULL)
                ThrowSystemError("Failed to create IOCP instance");

            nonConnected = addrs;
            asyncHandler = &handler;
            connectionLimit = connLimit;

            try
            {
                connectingThread.Start();
                workerThread.Start();
            }
            catch (...)
            {
                Stop();

                throw;
            }
        }

        void WinAsyncClientPool::Stop()
        {
            InternalStop();
        }

        void WinAsyncClientPool::InternalStop()
        {
            stopping = true;
            asyncHandler = 0;
            connectingThread.Stop();

            {
                common::concurrent::CsLockGuard lock(clientsCs);
                nonConnected.clear();
                clientAddrMap.clear();

                std::map<uint64_t, SP_WinAsyncClient>::iterator it;
                for (it = clientIdMap.begin(); it != clientIdMap.end(); ++it)
                {
                    WinAsyncClient& client = *it->second.Get();

                    client.Shutdown();
                }

                if (!clientIdMap.empty())
                {
//                    std::cout << "=============== WinAsyncClientPool: Waiting for empty" << std::endl;

                    clientsCv.Wait(clientsCs);
                }
            }

            workerThread.Stop();

            CloseHandle(iocp);
            iocp = NULL;
        }

        bool WinAsyncClientPool::isConnectionNeededLocked() const
        {
            return !nonConnected.empty() && (connectionLimit == 0 || connectionLimit > clientAddrMap.size());
        }

        uint64_t WinAsyncClientPool::AddClient(SP_WinAsyncClient& client)
        {
            uint64_t id;
            {
                WinAsyncClient& clientRef = *client.Get();

                common::concurrent::CsLockGuard lock(clientsCs);

                id = ++idGen;
                clientRef.SetId(id);

                HANDLE iocp0 = clientRef.AddToIocp(iocp);
                if (iocp0 == NULL)
                    ThrowSystemError("Can not add socket to IOCP");

                iocp = iocp0;

                clientIdMap[id] = client;
                clientAddrMap[clientRef.GetAddress()] = client;
                nonConnected.erase(std::find(nonConnected.begin(), nonConnected.end(), clientRef.GetRange()));
            }

            return id;
        }

        bool WinAsyncClientPool::Send(uint64_t id, impl::interop::SP_InteropMemory mem)
        {
//            std::cout << "=============== Send: " << id << std::endl;
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                if (stopping)
                    return false;

                client = FindClientLocked(id);
                if (!client.IsValid())
                    return false;
            }

//            std::cout << "=============== Send: Client found" << std::endl;
            return client.Get()->Send(mem);
        }

        bool WinAsyncClientPool::CloseAndRelease(uint64_t id)
        {
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                std::map<uint64_t, SP_WinAsyncClient>::iterator it = clientIdMap.find(id);
                if (it == clientIdMap.end())
                    return false;

                client = it->second;

                if (!client.Get()->IsClosed())
                {
                    clientAddrMap.erase(client.Get()->GetAddress());
                    nonConnected.push_back(client.Get()->GetRange());

                    connectingThread.WakeUp();
                }

                clientIdMap.erase(it);

//                std::cout << "=============== WorkerThread: clientIdMap.size=" << clientIdMap.size() << std::endl;
                if (clientIdMap.empty())
                {
//                    std::cout << "=============== WorkerThread: Notifying about empty" << std::endl;
                    clientsCv.NotifyAll();
                }
            }

            client.Get()->WaitForPendingIo();
            client.Get()->Close();

            return true;
        }

        void WinAsyncClientPool::CloseAndRelease(uint64_t id, const IgniteError* err)
        {
            bool found = CloseAndRelease(id);

            if (found && asyncHandler)
                asyncHandler->OnConnectionClosed(id, err);
        }

        bool WinAsyncClientPool::Close(uint64_t id)
        {
            SP_WinAsyncClient client;
            {
                common::concurrent::CsLockGuard lock(clientsCs);

                if (stopping)
                    return false;

                client = FindClientLocked(id);

                if (!client.IsValid() || client.Get()->IsClosed())
                    return false;

                clientAddrMap.erase(client.Get()->GetAddress());
                nonConnected.push_back(client.Get()->GetRange());

                // Leave client in clientIdMap until close event is received. Client instances contain OVERLAPPED
                // structures, which must not be freed until all async operations are complete.

                connectingThread.WakeUp();
            }

            client.Get()->Shutdown();

            return true;
        }

        void WinAsyncClientPool::Close(uint64_t id, const IgniteError* err)
        {
            bool found = Close(id);

            if (found && asyncHandler)
                asyncHandler->OnConnectionClosed(id, err);
        }

        void WinAsyncClientPool::ThrowSystemError(const std::string& msg)
        {
            std::stringstream buf;

            buf << "Windows system error: " << msg << ", system error code: " << GetLastError();

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        SP_WinAsyncClient WinAsyncClientPool::FindClientLocked(uint64_t id) const
        {
            std::map<uint64_t, SP_WinAsyncClient>::const_iterator it = clientIdMap.find(id);
            if (it == clientIdMap.end())
                return SP_WinAsyncClient();

            return it->second;
        }
    }
}
