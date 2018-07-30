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

import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;

@MXBeanDescription("MBean that provides ability to control delays of exchange messages for testing.")
public interface TestUnresponsiveExchangeMessagesControlMXBean {
    /**
     *
     */
    @MXBeanDescription("")
    public long getExchangeMessageSendDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("")
    public void setExchangeMessageSendDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("")
    public long getExchangeMessageReceiveDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("")
    public void setExchangeMessageReceiveDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("")
    public long getCheckMessageSendDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("")
    public void setCheckMessageSendDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("")
    public long getCheckMessageReceiveDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("")
    public void setCheckMessageReceiveDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("")
    public int getCheckMessageSendOverrideTopologyVersion();

    /**
     *
     */
    @MXBeanDescription("")
    public void setCheckMessageSendOverrideTopologyVersion(int majorVer);

    /**
     *
     */
    @MXBeanDescription("")
    public int getCheckMessageReceiveOverrideTopologyVersion();

    /**
     *
     */
    @MXBeanDescription("")
    public void setCheckMessageReceiveOverrideTopologyVersion(int majorVer);
}
