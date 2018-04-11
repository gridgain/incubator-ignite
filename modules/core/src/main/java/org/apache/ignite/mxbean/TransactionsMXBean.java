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

package org.apache.ignite.mxbean;

import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.transactions.Transaction;

/**
 * Transactions MXBean interface.
 */
@MXBeanDescription("MBean that provides access to Ignite transactions.")
public interface TransactionsMXBean {
    /**
     * Returns active transactions initiated by this node.
     *
     * @return Transactions started on local node.
     */
    @MXBeanDescription("All local active transactions.")
    public Map<String, String> getAllLocalTransactions();

    /**
     * Returns long-running active transactions initiated by this node.
     *
     * @param duration Duration, at least (ms).
     * @return Long-running transactions started on local node.
     */
    @MXBeanDescription("Long running local active transactions.")
    @MXBeanParametersNames("duration")
    @MXBeanParametersDescriptions("Duration, at least (ms).")
    public Map<String, String> getLongRunningLocalTransactions(int duration);

    /**
     * Stops a transaction initiated by this node.
     *
     * @param txId Transaction id to stop.
     * @return Status of transaction after calling {@link Transaction#close}.
     */
    @MXBeanDescription("Stop transaction.")
    @MXBeanParametersNames("txId")
    @MXBeanParametersDescriptions("Transaction id to stop.")
    public String stopTransaction(String txId) throws IgniteCheckedException;
}
