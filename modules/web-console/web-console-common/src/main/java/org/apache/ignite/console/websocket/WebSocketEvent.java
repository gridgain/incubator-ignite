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

package org.apache.ignite.console.websocket;

import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Data transfer object for websocket event.
 */
public class WebSocketEvent {
    /** */
    private String reqId;

    /** */
    private String evtType;

    /** */
    private String payload;

    /**
     * Default constructor for serialization.
     */
    public WebSocketEvent() {
        // No-op.
    }

    /**
     * Full constructor.
     *
     * @param reqId Request ID.
     * @param evtType Event type.
     * @param payload Payload.
     */
    public WebSocketEvent(String reqId, String evtType, String payload) {
        this.reqId = reqId;
        this.evtType = evtType;
        this.payload = payload;
    }

    /**
     * @return Request ID.
     */
    public String getRequestId() {
        return reqId;
    }

    /**
     * @param reqId New request ID.
     */
    public void setRequestId(String reqId) {
        this.reqId = reqId;
    }

    /**
     * @return Event type.
     */
    public String getEventType() {
        return evtType;
    }

    /**
     * @param evtType New event type.
     */
    public void setEventType(String evtType) {
        this.evtType = evtType;
    }

    /**
     * @return Payload.
     */
    public String getPayload() {
        return payload;
    }

    /**
     * @param payload New payload.
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketEvent.class, this);
    }
}
