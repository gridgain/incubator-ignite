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

package org.apache.ignite.internal;

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
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class TestUnresponsiveExchangeTcpCommunicationSpi extends TcpCommunicationSpi {
    /** Block exchange messages. */
    public volatile long exchangeMessagesSndDelay;

    /** Block exchange messages. */
    public volatile long exchangeMessagesRcvDelay;

    /** Block check messages. */
    public volatile long checkMessagesSndDelay;

    public volatile long checkMessagesRcvDelay;

    /** Send outdated check messages. */
    public volatile AffinityTopologyVersion checkMessagesSndTopVer;

    public volatile AffinityTopologyVersion checkMessagesRcvTopVer;

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg,
        IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
        Message msg0 = ((GridIoMessage)msg).message();

        try {
            if ((msg0 instanceof GridDhtPartitionsSingleMessage || msg0 instanceof GridDhtPartitionsFullMessage)
                && exchangeMessagesSndDelay != 0) {
                if (exchangeMessagesSndDelay > 0)
                    Thread.sleep(exchangeMessagesSndDelay);
                else
                    return;
            }

            if ((msg0 instanceof PartitionsExchangeFinishedCheckRequest ||
                msg0 instanceof PartitionsExchangeFinishedCheckResponse) && checkMessagesSndDelay != 0) {
                if (checkMessagesSndDelay > 0)
                    Thread.sleep(checkMessagesSndDelay);
                else
                    return;
            }

            if (msg0 instanceof PartitionsExchangeFinishedCheckResponse && checkMessagesSndTopVer != null)
                GridTestUtils.setFieldValue(msg0, "topVer", checkMessagesSndTopVer);
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
                && exchangeMessagesRcvDelay != 0) {
                if (exchangeMessagesRcvDelay > 0)
                    Thread.sleep(exchangeMessagesRcvDelay);
                else
                    return;
            }

            if ((msg0 instanceof PartitionsExchangeFinishedCheckRequest ||
                msg0 instanceof PartitionsExchangeFinishedCheckResponse) && checkMessagesRcvDelay > 0) {
                if (checkMessagesRcvDelay > 0)
                    Thread.sleep(checkMessagesRcvDelay);
                else
                    return;
            }

            if (msg0 instanceof PartitionsExchangeFinishedCheckResponse && checkMessagesRcvTopVer != null)
                GridTestUtils.setFieldValue(msg0, "topVer", checkMessagesRcvTopVer);
        }
        catch (InterruptedException ex) {
            throw new IgniteException(ex);
        }

        super.notifyListener(sndId, msg, msgC);
    }
}