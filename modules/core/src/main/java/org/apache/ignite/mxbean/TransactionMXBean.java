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

import org.apache.ignite.configuration.TransactionConfiguration;

/**
 * MBean that provides access to transaction management.
 */
@MXBeanDescription("MBean that provides access to transaction management.")
public interface TransactionMXBean {
    /**
     * Gets transaction timeout on partition map exchange.
     * @see TransactionConfiguration#getTxTimeoutOnPartitionMapExchange
     *
     * @return Transaction timeout on partition map exchange in milliseconds.
     */
    @MXBeanDescription(
        "Transaction timeout on partition map exchange in milliseconds. " +
        "If not set, default value is TransactionConfiguration#TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE which means " +
        "transactions will never be rolled back on partition map exchange."
    )
    public long getTxTimeoutOnPartitionMapExchange();

    /**
     * Sets transaction timeout on partition map exchange.
     * If not set, default value is {@link TransactionConfiguration#TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE} which means transactions will never be
     * rolled back on partition map exchange.
     * @see TransactionConfiguration#setTxTimeoutOnPartitionMapExchange
     *
     * @param timeout Transaction timeout on partition map exchange in milliseconds.
     */
    @MXBeanDescription(
        "Sets transaction timeout on partition map exchange in milliseconds. " +
        "If not set, default value is TransactionConfiguration#TX_TIMEOUT_ON_PARTITION_MAP_EXCHANGE which means " +
        "transactions will never be rolled back on partition map exchange."
    )
    @MXBeanParametersNames(
        "timeout"
    )
    @MXBeanParametersDescriptions(
        "Transaction timeout on partition map exchange in milliseconds."
    )
    public void setTxTimeoutOnPartitionMapExchange(long timeout);
}
