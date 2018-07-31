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

/**
 *
 */
@MXBeanDescription("MBean that provides ability to control delays of exchange messages for testing.")
public interface TestUnresponsiveExchangeMessagesControlMXBean {
    /**
     *
     */
    @MXBeanDescription("Exchange message send delay")
    public long getExchangeMessageSendDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("Write exchange message send delay")
    public void writeExchangeMessageSendDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("Exchange message receive delay")
    public long getExchangeMessageReceiveDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("Write exchange message receive delay")
    public void writeExchangeMessageReceiveDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("Check message send delay")
    public long getCheckMessageSendDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("Write check message send delay")
    public void writeCheckMessageSendDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("Check message receive delay")
    public long getCheckMessageReceiveDelay();

    /**
     * @param delay Delay.
     */
    @MXBeanDescription("Write check message receive delay")
    public void writeCheckMessageReceiveDelay(long delay);

    /**
     *
     */
    @MXBeanDescription("Override topology version for check message on sender")
    public int getCheckMessageSendOverrideTopologyVersion();

    /**
     *
     */
    @MXBeanDescription("Set override topology version for check message on sender")
    public void writeCheckMessageSendOverrideTopologyVersion(int majorVer);

    /**
     *
     */
    @MXBeanDescription("Override topology version for check message on receiver")
    public int getCheckMessageReceiveOverrideTopologyVersion();

    /**
     *
     */
    @MXBeanDescription("Set override topology version for check message on receiver")
    public void writeCheckMessageReceiveOverrideTopologyVersion(int majorVer);
}
