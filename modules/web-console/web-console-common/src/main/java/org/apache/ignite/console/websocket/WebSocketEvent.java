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
 * TODO Move to ignite-web-console-common module?
 */
public class WebSocketEvent {
    /** */
    private String requestId;

    /** */
    private String evtType;

    /** */
    private String data;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String id) {
        this.requestId = id;
    }

    public String getEventType() {
        return evtType;
    }

    public void setEventType(String evt) {
        this.evtType = evt;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(WebSocketEvent.class, this);
    }
}
