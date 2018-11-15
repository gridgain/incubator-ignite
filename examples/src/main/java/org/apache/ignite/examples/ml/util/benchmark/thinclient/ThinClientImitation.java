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
import java.util.List;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.ClientInputStream;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.ClientOutputStream;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Connection;
import org.apache.ignite.examples.ml.util.benchmark.thinclient.utils.Measure;

public class ThinClientImitation {
    private static final int SAMPLES = 20;
    private static final int SCAN_QUERY_HEADER_SIZE = 25;
    private static final int READ_NEXT_PAGE_SIZE = 17;

    private static final int[] pageSizes = new int[] {5, 10, 20, 50, 100, 150, 200, 300, 400, 500, 600};

    public static void main(String... args) throws Exception {
        for (int pageSize : pageSizes) {
            ArrayList<Measure> times = new ArrayList<>(SAMPLES);
            for (int i = 0; i < SAMPLES; i++) {
                try (Connection connection = new Connection(new Socket("localhost", 10800))) {
                    long start = System.currentTimeMillis();
                    FirstQueryResponse firstQueryResponse = handshakeAndQuery(connection, pageSize);
                    List<PageResponse> nextPages = new ArrayList<>();
                    boolean isLastPage = firstQueryResponse.isLastPage;
                    while(!isLastPage) {
                        PageResponse response = readNextPage(connection, firstQueryResponse);
                        nextPages.add(response);
                        isLastPage = response.isLastPage;
                    }
                    long end = System.currentTimeMillis();
                    long dataReceiveTime = end - start;
                    long totalPayload = (firstQueryResponse.pageSize + nextPages.stream().mapToInt(x -> x.pageSize).sum()) / 1024;

                    times.add(new Measure(firstQueryResponse.latency, totalPayload / (dataReceiveTime / 1000)));
                }
            }

            Measure.computeStatsAndPrint(pageSize, times);
        }
    }

    private static FirstQueryResponse handshakeAndQuery(Connection connection, int pageSize) throws IOException {
        FirstQueryResponse response = null;
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

        // Scan Query.
        out.writeInt(25);                    // Message length.
        out.writeShort(2000);                // Scan Query opcode.
        out.writeLong(0);                    // Request ID.
        out.writeInt(ThinClientImitationServer.CACHE_NAME.hashCode());  // Cache Name hash code.
        out.writeByte(0);                    // Flags.
        out.writeByte(101);                  // Filter (NULL).
        out.writeInt(pageSize);                 // Page Size.
        out.writeInt(-1);                    // Partition to query.
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

        int pageSizeInMsg = msgLen - SCAN_QUERY_HEADER_SIZE;

        readPageToNULL(in, pageSizeInMsg);

        boolean isLastPage = in.readByte() == 0;
        return new FirstQueryResponse(latency, cursorID, rowCount, pageSizeInMsg, isLastPage);
    }


    private static void readPageToNULL(ClientInputStream in, int pageSize) throws IOException {
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
        }
    }

    private static PageResponse readNextPage(Connection connection, FirstQueryResponse firstQueryResponse) throws IOException {
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

        int pageSizeInMsg = msgLen - READ_NEXT_PAGE_SIZE;

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
