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

package org.apache.ignite.internal.processors.trace;

/**
 *
 */
public interface IgniteTraceAware {
    /**
     *
     */
    public enum TracePoint {
        TX_CREATE,
        TX_PREPARE,
        TX_COMMIT,
        TX_END,
        MSG_NIO_RECEIVE,
        MSG_NIO_SEND,
        MSG_LISTENER_INVOKE,
        NEAR_LOCK_REQUEST_CREATED,
        NEAR_LOCK_RESPONSE_CREATED,
        NEAR_PREPARE_REQUEST_CREATED,
        NEAR_PREPARE_RESPONSE_CREATED,
        DHT_LOCK_REQUEST_CREATED,
        DHT_LOCK_RESPONSE_CREATED,
        DHT_PREPARE_REQUEST_CREATED,
        DHT_PREPARE_RESPONSE_CREATED,
        NEAR_FINISH_REQUEST_CREATED,
        NEAR_FINISH_RESPONSE_CREATED,
        DHT_FINISH_REQUEST_CREATED,
        DHT_FINISH_RESPONSE_CREATED,
        TX_LOCAL_COMMIT_BEGIN,
        TX_LOCAL_COMMIT_END,
        TX_LOCAL_WAL_SYNC_BEGIN,
        TX_LOCAL_WAL_SYNC_END,
        TX_LOCAL_CHECKPOINT_LOCK_ACQUIRED
        ;

        /** */
        private static final TracePoint[] VALS = values();

        /**
         * @param ord Ordinal.
         * @return TracePoint enum.
         */
        public static TracePoint fromOrdinal(int ord) {
            return ord < 0 || ord >= VALS.length ? null : VALS[ord];
        }
    }

    /**
     * @param point Trace point.
     */
    public void recordTracePoint(TracePoint point);
}
