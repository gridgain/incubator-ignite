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

package org.apache.ignite.spi.deployment.uri;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.spi.GridSpiAbstractConfigTest;
import org.apache.ignite.testframework.junits.spi.GridSpiTest;
import org.junit.Test;

/**
 *
 */
@GridSpiTest(spi = UriDeploymentSpi.class, group = "Deployment SPI")
public class GridUriDeploymentConfigSelfTest extends GridSpiAbstractConfigTest<UriDeploymentSpi> {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testNegativeConfig() throws Exception {
        checkNegativeSpiProperty(new UriDeploymentSpi(), "uriList", null);
        checkNegativeSpiProperty(new UriDeploymentSpi(), "uriList", Collections.singletonList("qwertyuiop"), false);
        checkNegativeSpiProperty(new UriDeploymentSpi(), "uriList", Collections.singletonList(null), false);
    }

    /**
     *
     * @throws Exception
     */
    @Test
    public void testClientSpiConsistencyChecked() throws Exception {
        IgniteConfiguration scfg = super.getConfiguration();

        UriDeploymentSpi deploymentSpi = new UriDeploymentSpi();
        String tmpDir = GridTestProperties.getProperty("deploy.uri.tmpdir");
        File tmp = new File(tmpDir);
        if (!tmp.exists()) {
            tmp.mkdir();
        }
        deploymentSpi.setUriList(Arrays.asList("file://" + tmpDir));
        scfg.setDeploymentSpi(deploymentSpi);
        startGrid("server", scfg);

        IgniteConfiguration ccfg = super.getConfiguration();
        startClientGrid("client", ccfg);

        stopGrid("server");
        stopGrid("client");
    }
}
