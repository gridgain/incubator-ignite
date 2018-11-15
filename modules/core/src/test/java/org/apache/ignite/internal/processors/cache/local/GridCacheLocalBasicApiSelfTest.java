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

package org.apache.ignite.internal.processors.cache.local;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheBasicApiAbstractTest;
import org.apache.ignite.testframework.MvccFeatureChecker;

import static org.apache.ignite.cache.CacheMode.LOCAL;

/**
 * Basic API tests.
 */
public class GridCacheLocalBasicApiSelfTest extends GridCacheBasicApiAbstractTest {
    /** {@inheritDoc} */
    @Override protected void setUp() throws Exception {
        MvccFeatureChecker.failIfNotSupported(MvccFeatureChecker.Feature.LOCAL_CACHE);

        super.setUp();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }
}