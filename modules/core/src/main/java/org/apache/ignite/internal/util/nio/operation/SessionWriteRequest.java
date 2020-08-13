/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util.nio.operation;

import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.nio.GridNioSession;

/**
 *
 */
public interface SessionWriteRequest extends SessionOperationRequest {
    /**
     * @return {@code True} if future was created in thread that was processing message.
     */
    public boolean skipBackPressure();

    /**
     * @return Whether the message is system.
     */
    boolean system();

    /**
     * @return Message.
     */
    public Object message();

    /**
     * Stored span for tracing.
     *
     * @return Span.
     */
    Span span();

    /** */
    public void onMessageWritten();

    /**
     * The method will be called when ack received.
     */
    public void onAckReceived();

    /** */
    public void onError(Exception e);

    /**
     * @param ses New session.
     */
    public SessionWriteRequest recover(GridNioSession ses);
}
