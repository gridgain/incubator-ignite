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

package org.apache.ignite.internal.util.nio;

import java.io.IOException;
import org.apache.ignite.IgniteCheckedException;

/**
 *
 */
interface SessionChangeRequest {
    /**
     * @return Session.
     */
    <T extends GridNioSession> T session();

    /**
     * @return Requested change operation.
     */
    NioOperation operation();

    /**
     * Invokes the change operation.
     * It is guaranteed that the method invokes from Nio thread.
     * It's possible that method invokes several times in certain
     * cases but guaranteed that it isn't called concurrently.
     * @param nio Nio server.
     * @param worker Nio worker.
     * @throws IOException If failed.
     * @throws IgniteCheckedException If failed.
     */
    <T> void invoke(GridNioServer<T> nio, GridNioWorker worker) throws IOException, IgniteCheckedException;
}
