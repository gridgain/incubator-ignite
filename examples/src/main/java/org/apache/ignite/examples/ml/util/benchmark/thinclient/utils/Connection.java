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

package org.apache.ignite.examples.ml.util.benchmark.thinclient.utils;

import java.io.IOException;
import java.net.Socket;

public class Connection implements AutoCloseable {
    private final Socket socket;
    private final ClientInputStream in;
    private final ClientOutputStream out;

    public Connection(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new ClientInputStream(socket.getInputStream());
        this.out = new ClientOutputStream(socket.getOutputStream());
    }

    public ClientInputStream in() {
        return in;
    }

    public ClientOutputStream out() {
        return out;
    }

    @Override public void close() throws Exception {
        socket.close();
    }
}
