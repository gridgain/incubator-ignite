/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.compatibility.persistence.scenarios.latest;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.compatibility.persistence.api.PdsCompatibilityLatestNodeScenario;
import org.apache.ignite.compatibility.persistence.scenarios.util.PdsCompatibilityBasicUtils;

/**
 * Scenario for checking basic PDS functionality compatibility.
 */
public class LatestNodePdsBasicScenarioImpl implements PdsCompatibilityLatestNodeScenario {
    /** {@inheritDoc} */
    @Override public IgniteConfiguration configure() {
        return PdsCompatibilityBasicUtils.configuration();
    }

    /** {@inheritDoc} */
    @Override public void validate(Ignite ignite) {
        PdsCompatibilityBasicUtils.validateData(ignite);
    }
}
