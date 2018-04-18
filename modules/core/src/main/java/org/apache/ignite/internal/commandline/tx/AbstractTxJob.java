/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ignite.internal.commandline.tx;

import java.util.regex.Pattern;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

/**
 *
 */
public abstract class AbstractTxJob extends ComputeJobAdapter {
    /** */
    protected final long durationMillis;
    /** */
    protected final long size;
    /** */
    protected final Pattern lbPat;
    /** */
    @IgniteInstanceResource
    protected IgniteEx ignite;
    /** */
    @LoggerResource
    protected IgniteLogger log;

    /**
     * @param arg Argument.
     */
    public AbstractTxJob(TransactionsTaskArguments arg) {
        this.durationMillis = arg.duration() * 1000;
        this.size = arg.size();
        this.lbPat = arg.labelPattern();
    }

    /**
     * @param curTime Current time.
     * @param tx Transaction.
     * @return check transaction for conditions
     */
    protected boolean checkTx(long curTime, GridNearTxLocal tx) {
        if (durationMillis > 0 && (curTime - tx.startTime()) < durationMillis) {
            if (log.isTraceEnabled()) {
                ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                log.trace("filtered out by " + durationMillis + " duration: " + TxDTO.of(node, tx));
            }
            return false;
        }

        if (size > 0 && tx.size() < size) {
            if (log.isTraceEnabled()) {
                ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                log.trace("filtered out by " + size + " size: " + TxDTO.of(node, tx));
            }
            return false;
        }

        if (lbPat != null && !lbPat.matcher(tx.label()).matches()) {
            if (log.isTraceEnabled()) {
                ClusterNode node = ignite.context().discovery().node(tx.nodeId());
                log.trace("filtered out by " + lbPat.pattern() + " label: " + TxDTO.of(node, tx));
            }
            return false;
        }
        return true;
    }
}
