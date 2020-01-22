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

package org.apache.ignite.internal.processors.configuration.distributed;

import java.util.function.Consumer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;

/**
 * TODO: Add class description.
 *
 * @author @java.author
 * @version @java.version
 */
public class TestDistibutedConfigurationPlugin extends AbstractTestPluginProvider {
    /** */
    private GridKernalContext igniteCtx;

    public static Consumer<GridKernalContext> supplier = (ctx) -> {
    };

    /**
     * Clear current supplier.
     */
    public static void clear() {
        supplier = (ctx) -> {
        };
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "TestDistibutedConfigurationPlugin";
    }

    /** {@inheritDoc} */
    @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
        igniteCtx = ((IgniteKernal)ctx.grid()).context();
    }

    /** {@inheritDoc} */
    @Override public void start(PluginContext ctx) throws IgniteCheckedException {
        supplier.accept(igniteCtx);
    }
}
