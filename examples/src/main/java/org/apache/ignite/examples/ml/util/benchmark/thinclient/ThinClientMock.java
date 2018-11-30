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

package org.apache.ignite.examples.ml.util.benchmark.thinclient;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.ClientInputStream;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.ClientOutputStream;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Connection;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Measure;
import org.apache.ignite.lang.IgniteBiTuple;

public class ThinClientMock {
    private static final int SCAN_QUERY_HEADER_MESSAGE_SIZE = 25;
    private static final int READ_NEXT_PAGE_MESSAGE_SIZE = 17;

    private static final String HOST = "localhost";
    private static final int PORT = 10800;

    private final int pageSize;
    private final List<Integer> partitions;

    public ThinClientMock(int pageSize, int clientId, int countOfClients, boolean distinguishPartitions, int countOfPartitions) {
        this.pageSize = pageSize;
        if (distinguishPartitions) {
            partitions = IntStream.range(0, countOfPartitions)
                .filter(partition -> (partition % countOfClients) == clientId).boxed()
                .collect(Collectors.toList());
        }
        else
            partitions = Collections.singletonList(-1);
    }

    public Optional<Measure> measure() throws Exception {
        try (Connection connection = new Connection(new Socket(HOST, PORT))) {
            long start = System.currentTimeMillis();

            long firstQueryLatency = -1L;
            double totalPayload = 0L;
            handshake(connection);
            for (Integer partId : partitions) {
                IgniteBiTuple<Long, Long> result = getOnePartition(connection, partId);
                if (firstQueryLatency == -1L)
                    firstQueryLatency = result.get1();
                totalPayload += result.get2();
            }

            long end = System.currentTimeMillis();
            long dataReceiveTime = end - start;
            double seconds = dataReceiveTime / 1000;
            if(seconds < 0)
                throw new IllegalStateException();

            return Optional.of(new Measure(firstQueryLatency, totalPayload / seconds));
        }
    }

    //returns latency and total payload in kbytes
    private IgniteBiTuple<Long, Long> getOnePartition(Connection connection, int partitionId) throws IOException {
        FirstQueryResponse firstQueryResponse = firstQuery(connection, pageSize, partitionId);
        List<PageResponse> nextPages = new ArrayList<>();
        boolean isLastPage = firstQueryResponse.isLastPage;
        while (!isLastPage) {
            PageResponse response = readNextPage(connection, firstQueryResponse);
            nextPages.add(response);
            isLastPage = response.isLastPage;
        }
        long totalPayload = (firstQueryResponse.pageSize + nextPages.stream().mapToInt(x -> x.pageSize).sum()) / 1024;
        closeCursor(connection, firstQueryResponse.cursorId);
        return new IgniteBiTuple<>(firstQueryResponse.latency, totalPayload);
    }


    private void handshake(Connection connection) throws IOException {
        ClientOutputStream out = connection.out();
        ClientInputStream in = connection.in();

        // Handshake.
        out.writeInt(8);                     // Message length.
        out.writeByte(1);                    // Always 1.
        out.writeShort(1);                   // Version major.
        out.writeShort(1);                   // Version minor.
        out.writeShort(0);                   // Version patch.
        out.writeByte(2);                    // Always 2.
        out.flush();

        in.readInt();                          // Handshake result length.
        int res = in.readByte();               // Handshake result.
        if (res != 1)
            throw new IllegalStateException("Handshake failed [res=" + res + "]");
    }

    private FirstQueryResponse firstQuery(Connection connection, int pageSize, int partitionId) throws IOException {
        ClientOutputStream out = connection.out();
        ClientInputStream in = connection.in();

        // Scan Query.
        out.writeInt(25);                    // Message length.
        out.writeShort(2000);                // Scan Query opcode.
        out.writeLong(0);                    // Request ID.
        out.writeInt(ServerMock.CACHE_NAME.hashCode());  // Cache Name hash code.
        out.writeByte(0);                    // Flags.
        out.writeByte(101);                  // Filter (NULL).
        out.writeInt(pageSize);                 // Page Size.
        out.writeInt(partitionId);              // Partition to query.
        out.writeByte(1);                    // Local flag.
        out.flush();

        long start = System.currentTimeMillis();

        int msgLen = in.readInt();                          // Result length.

        long end = System.currentTimeMillis();
        long latency = end - start;

        if (msgLen == 0)
            throw new RuntimeException("Empty message");

        in.readLong(); //request ID

        if (in.readInt() != 0) //status code
            throw new RuntimeException("Non-zero status code");

        long cursorID = in.readLong(); //cursor ID
        int rowCount = in.readInt(); //row cnt

        int pageSizeInMsg = msgLen - SCAN_QUERY_HEADER_MESSAGE_SIZE;

        readPageToNULL(in, pageSizeInMsg);

        boolean isLastPage = in.readByte() == 0;
        return new FirstQueryResponse(latency, cursorID, rowCount, pageSizeInMsg, isLastPage);
    }

    private void readPageToNULL(ClientInputStream in, int pageSize) throws IOException {
        int BUFFER_LEN = 1024 * 1024;
        byte[] buffer = new byte[BUFFER_LEN];
        int received = 0;

        while (received < pageSize) {
            int res = in.in().read(buffer, 0, Math.min(buffer.length, pageSize - received));
            if (res < 0)
                throw new RuntimeException("EOF");
            if (res == 0)
                throw new RuntimeException("Server closed connection");

            received += res;
            Benchmark.downloadedBytes.addAndGet(res);
        }
    }

    private void closeCursor(Connection connection, long cursorId) throws IOException {
//        try {
//            ClientOutputStream out = connection.out();
//            out.writeInt(18); //msg lengh
//            out.writeShort(0); //close resource
//            out.writeLong(0); //req id
//            out.writeLong(cursorId); //cursor ID
//            out.flush();
//        }
    }

    private PageResponse readNextPage(Connection connection, FirstQueryResponse firstQueryResponse) throws IOException {
        ClientOutputStream out = connection.out();
        ClientInputStream in = connection.in();

        out.writeInt(18); //req len
        out.writeShort(2001); //get next page opcode
        out.writeLong(0); //req id
        out.writeLong(firstQueryResponse.cursorId);
        out.flush();

        long start = System.currentTimeMillis();

        int msgLen = in.readInt();                          // Result length.

        long end = System.currentTimeMillis();
        long latency = end - start;

        if (msgLen == 0)
            throw new RuntimeException("Empty message");

        in.readLong(); //request ID

        if (in.readInt() != 0) //status code
            throw new RuntimeException("Non-zero status code");

        int rowCount = in.readInt(); //row cnt

        int pageSizeInMsg = msgLen - READ_NEXT_PAGE_MESSAGE_SIZE;

        readPageToNULL(in, pageSizeInMsg);

        boolean isLastPage = in.readByte() == 0;
        return new PageResponse(latency, pageSizeInMsg, rowCount, isLastPage);
    }

    private static class FirstQueryResponse {
        private final long latency;
        private final long pageSize;
        private final long cursorId;
        private final int rowsOnPage;
        private final boolean isLastPage;

        public FirstQueryResponse(long latency, long cursorId, int rowsOnPage, long pageSize, boolean isLastPage) {
            this.latency = latency;
            this.cursorId = cursorId;
            this.pageSize = pageSize;
            this.isLastPage = isLastPage;
            this.rowsOnPage = rowsOnPage;
        }
    }

    private static class PageResponse {
        private final long latency;
        private final int pageSize;
        private final int rowsOnPage;
        private final boolean isLastPage;

        public PageResponse(long latency, int pageSize, int rowsOnPage, boolean isLastPage) {
            this.latency = latency;
            this.pageSize = pageSize;
            this.rowsOnPage = rowsOnPage;
            this.isLastPage = isLastPage;
        }
    }
}
