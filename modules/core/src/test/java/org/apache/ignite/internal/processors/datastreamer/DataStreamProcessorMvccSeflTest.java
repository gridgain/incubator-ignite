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

package org.apache.ignite.internal.processors.datastreamer;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

public class DataStreamProcessorMvccSeflTest extends DataStreamProcessorSelfTest {
    @Override
    protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration igniteConfiguration = super.getConfiguration(igniteInstanceName);
        CacheConfiguration[] cacheConfigurations = igniteConfiguration.getCacheConfiguration();
        assert cacheConfigurations == null || cacheConfigurations.length == 0
                || (cacheConfigurations.length == 1 && cacheConfigurations[0].getAtomicityMode() == TRANSACTIONAL);
        igniteConfiguration.setMvccEnabled(true);
        return igniteConfiguration;
    }

    @Override
    public void testPartitioned() throws Exception {
        // TODO "https://issues.apache.org/jira/browse/IGNITE-8149"
    }

    @Override
    public void testColocated() throws Exception {
        // TODO "https://issues.apache.org/jira/browse/IGNITE-8149"
    }

    @Override
    public void testReplicated() throws Exception {
        // TODO "https://issues.apache.org/jira/browse/IGNITE-8149"
    }

    @Override
    public void testUpdateStore() throws Exception {
        // TODO https://issues.apache.org/jira/browse/IGNITE-8582
    }
}
