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

#include <ignite/impl/binary/binary_utils.h>

#include "network/sockets.h"
#include "network/win_async_client.h"

namespace ignite
{
    namespace network
    {
        WinAsyncClient::WinAsyncClient(SOCKET socket, const EndPoint &addr, const TcpRange& range, int32_t bufLen) :
            bufLen(bufLen),
            state(State::CONNECTED),
            socket(socket),
            id(0),
            addr(addr),
            range(range)
        {
            memset(&currentSend, 0, sizeof(currentSend));
            currentSend.kind = IoOperationKind::SEND;

            memset(&currentRecv, 0, sizeof(currentRecv));
            currentRecv.kind = IoOperationKind::RECEIVE;
        }

        WinAsyncClient::~WinAsyncClient()
        {
            if (State::IN_POOL == state)
            {
                Shutdown();

                WaitForPendingIo();
            }

            if (State::CLOSED != state)
                Close();

            // std::cout << "=============== " << "0000000000000000" << " " << " ~WinAsyncClient " << id << std::endl;
        }

        bool WinAsyncClient::Shutdown()
        {
            // std::cout << "=============== " << "0000000000000000" << " " << " WinAsyncClient::Shutdown " << id << std::endl;
            common::concurrent::CsLockGuard lock(sendCs);

            if (State::CONNECTED != state && State::IN_POOL != state)
                return false;

            shutdown(socket, SD_BOTH);

            state = State::SHUTDOWN;

            return true;
        }

        void WinAsyncClient::WaitForPendingIo()
        {
            // std::cout << "=============== " << "0000000000000000" << " " << " WinAsyncClient::WaitForPendingIo " << id << std::endl;
            while (!HasOverlappedIoCompleted(&currentSend.overlapped))
                GetOverlappedResult((HANDLE)socket, &currentSend.overlapped, NULL, TRUE);

            while (!HasOverlappedIoCompleted(&currentRecv.overlapped))
                GetOverlappedResult((HANDLE)socket, &currentRecv.overlapped, NULL, TRUE);
        }

        bool WinAsyncClient::Close()
        {
            // std::cout << "=============== " << "0000000000000000" << " " << " WinAsyncClient::Close " << id << std::endl;
            if (State::CLOSED == state)
                return false;

            closesocket(socket);

            sendPackets.clear();
            recvPacket = impl::interop::SP_InteropMemory();

            state = State::CLOSED;

            return true;
        }

        HANDLE WinAsyncClient::AddToIocp(HANDLE iocp)
        {
            assert(State::CONNECTED == state);

            HANDLE res = CreateIoCompletionPort((HANDLE)socket, iocp, reinterpret_cast<DWORD_PTR>(this), 0);

            if (!res)
                return res;

            state = State::IN_POOL;

            return res;
        }

        bool WinAsyncClient::Send(const DataBuffer& data)
        {
            common::concurrent::CsLockGuard lock(sendCs);

            if (State::CONNECTED != state && State::IN_POOL != state)
                return false;

            sendPackets.push_back(data);

            if (sendPackets.size() > 1)
                return true;

            return SendNextPacketLocked();
        }

        bool WinAsyncClient::SendNextPacketLocked()
        {
            if (sendPackets.empty())
                return true;

            const DataBuffer& packet0 = sendPackets.front();
            DWORD flags = 0;

            WSABUF buffer;
            buffer.buf = (CHAR*)packet0.GetData();
            buffer.len = packet0.GetSize();

            // std::cout << "=============== " << "0000000000000000" << " " << " Send to " << id << " " << buffer.len << " bytes" << std::endl;
            int ret = WSASend(socket, &buffer, 1, NULL, flags, &currentSend.overlapped, NULL);

            return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
        }

        bool WinAsyncClient::Receive()
        {
            // We do not need locking on receive as we're always reading in a single thread at most.
            // If this ever changes we'd need to add mutex locking here.
            if (State::CONNECTED != state && State::IN_POOL != state)
                return false;

            if (!recvPacket.IsValid())
                ClearReceiveBuffer();

            impl::interop::InteropMemory& packet0 = *recvPacket.Get();

            DWORD flags = 0;
            WSABUF buffer;
            buffer.buf = (CHAR*)packet0.Data();
            buffer.len = (ULONG)packet0.Length();

            // std::cout << "=============== " << "0000000000000000" << " " << " Recv from " << id << " " << buffer.len << " bytes" << std::endl;
            int ret = WSARecv(socket, &buffer, 1, NULL, &flags, &currentRecv.overlapped, NULL);

            return ret != SOCKET_ERROR || WSAGetLastError() == ERROR_IO_PENDING;
        }

        void WinAsyncClient::ClearReceiveBuffer()
        {
            using namespace impl::interop;

            if (!recvPacket.IsValid())
            {
                recvPacket = SP_InteropMemory(new InteropUnpooledMemory(bufLen));
                recvPacket.Get()->Length(bufLen);
            }
        }

        DataBuffer WinAsyncClient::ProcessReceived(size_t bytes)
        {
            // std::cout << "=============== " << "0000000000000000" << " " << " WinAsyncClient: bytes=" << bytes << std::endl;
            impl::interop::InteropMemory& packet0 = *recvPacket.Get();

            return DataBuffer(recvPacket, 0, static_cast<int32_t>(bytes));
        }

        bool WinAsyncClient::ProcessSent(size_t bytes)
        {
            common::concurrent::CsLockGuard lock(sendCs);

            DataBuffer& front = sendPackets.front();

            front.Skip(static_cast<int32_t>(bytes));

            if (front.IsEmpty())
                sendPackets.pop_front();

            return SendNextPacketLocked();
        }
    }
}
