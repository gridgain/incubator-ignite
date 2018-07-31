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

package org.apache.ignite.failure;

import java.lang.reflect.Field;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeFinishedCheckRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.PartitionsExchangeFinishedCheckResponse;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.mxbean.TestUnresponsiveExchangeMessagesControlMXBean;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;

/**
 *
 */
public class TestUnresponsiveExchangeTcpCommunicationSpi extends TcpCommunicationSpi
    implements TestUnresponsiveExchangeMessagesControlMXBean {
    /** Exchange message send delay. */
    private volatile long exchangeMsgSndDelay;

    /** Exchange message receive delay. */
    private volatile long exchangeMsgRcvDelay;

    /** Check message send delay. */
    private volatile long checkMsgSndDelay;

    /** Check message receive delay. */
    private volatile long checkMsgRcvDelay;

    /** Check message send topology version. */
    private volatile int checkMsgSndOverrideTopVer = -1;

    /** Check message receive topology version. */
    private volatile int checkMsgRcvOverrideTopVer = -1;

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg,
        IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
        Message msg0 = ((GridIoMessage)msg).message();

        try {
            if ((msg0 instanceof GridDhtPartitionsSingleMessage || msg0 instanceof GridDhtPartitionsFullMessage)
                && exchangeMsgSndDelay != 0) {
                if (exchangeMsgSndDelay > 0)
                    Thread.sleep(exchangeMsgSndDelay);
                else
                    return;
            }

            if ((msg0 instanceof PartitionsExchangeFinishedCheckRequest ||
                msg0 instanceof PartitionsExchangeFinishedCheckResponse) && checkMsgSndDelay != 0) {
                if (checkMsgSndDelay > 0)
                    Thread.sleep(checkMsgSndDelay);
                else
                    return;
            }



            if (msg0 instanceof PartitionsExchangeFinishedCheckResponse && checkMsgSndOverrideTopVer >= 0)
                setFieldValue(msg0, "topVer", new AffinityTopologyVersion(checkMsgSndOverrideTopVer, 0));
        }
        catch (InterruptedException ex) {
            throw new IgniteException(ex);
        }

        super.sendMessage(node, msg, ackC);

    }

    /** {@inheritDoc} */
    @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
        Message msg0 = ((GridIoMessage)msg).message();

        try {
            if ((msg0 instanceof GridDhtPartitionsSingleMessage || msg0 instanceof GridDhtPartitionsFullMessage)
                && exchangeMsgRcvDelay != 0) {
                if (exchangeMsgRcvDelay > 0)
                    Thread.sleep(exchangeMsgRcvDelay);
                else
                    return;
            }

            if ((msg0 instanceof PartitionsExchangeFinishedCheckRequest ||
                msg0 instanceof PartitionsExchangeFinishedCheckResponse) && checkMsgRcvDelay > 0) {
                if (checkMsgRcvDelay > 0)
                    Thread.sleep(checkMsgRcvDelay);
                else
                    return;
            }

            if (msg0 instanceof PartitionsExchangeFinishedCheckResponse && checkMsgRcvOverrideTopVer >= 0)
                setFieldValue(msg0, "topVer", new AffinityTopologyVersion(checkMsgRcvOverrideTopVer, 0));
        }
        catch (InterruptedException ex) {
            throw new IgniteException(ex);
        }

        super.notifyListener(sndId, msg, msgC);
    }

    /** {@inheritDoc} */
    @Override public long getExchangeMessageSendDelay() {
        return exchangeMsgSndDelay;
    }

    /** {@inheritDoc} */
    @Override public void writeExchangeMessageSendDelay(long delay) {
        exchangeMsgSndDelay = delay;
    }

    /** {@inheritDoc} */
    @Override public long getExchangeMessageReceiveDelay() {
        return exchangeMsgRcvDelay;
    }

    /** {@inheritDoc} */
    @Override public void writeExchangeMessageReceiveDelay(long delay) {
        exchangeMsgRcvDelay = delay;
    }

    /** {@inheritDoc} */
    @Override public long getCheckMessageSendDelay() {
        return checkMsgSndDelay;
    }

    /** {@inheritDoc} */
    @Override public void writeCheckMessageSendDelay(long delay) {
        checkMsgSndDelay = delay;
    }

    /** {@inheritDoc} */
    @Override public long getCheckMessageReceiveDelay() {
        return checkMsgRcvDelay;
    }

    /** {@inheritDoc} */
    @Override public void writeCheckMessageReceiveDelay(long delay) {
        checkMsgRcvDelay = delay;
    }

    /** {@inheritDoc} */
    @Override public int getCheckMessageSendOverrideTopologyVersion() {
        return checkMsgSndOverrideTopVer;
    }

    /** {@inheritDoc} */
    @Override public void writeCheckMessageSendOverrideTopologyVersion(int majorVer) {
        checkMsgSndOverrideTopVer = majorVer;
    }

    /** {@inheritDoc} */
    @Override public int getCheckMessageReceiveOverrideTopologyVersion() {
        return checkMsgRcvOverrideTopVer;
    }

    /** {@inheritDoc} */
    @Override public void writeCheckMessageReceiveOverrideTopologyVersion(int majorVer) {
        checkMsgRcvOverrideTopVer = majorVer;
    }

    /**
     * @param obj Object.
     * @param fieldName Field name.
     * @param val Value.
     */
    private static void setFieldValue(Object obj, String fieldName, Object val) throws IgniteException {
        assert obj != null;
        assert fieldName != null;

        try {
            Class<?> cls = obj instanceof Class ? (Class)obj : obj.getClass();

            Field field = cls.getDeclaredField(fieldName);

            synchronized (field) {
                // Backup accessible field state.
                boolean accessible = field.isAccessible();

                try {
                    if (!accessible)
                        field.setAccessible(true);

                    field.set(obj, val);
                }
                finally {
                    // Recover accessible field state.
                    if (!accessible)
                        field.setAccessible(false);
                }
            }
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IgniteException("Failed to set object field [obj=" + obj + ", field=" + fieldName + ']', e);
        }
    }
}