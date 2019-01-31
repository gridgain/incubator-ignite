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

package org.apache.ignite.console.di;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.AuthProvider;
import org.apache.ignite.Ignite;
import org.apache.ignite.console.auth.IgniteAuth;

/**
 * Module that will configure DI.
 */
public class Bindings extends AbstractModule {
    /** */
    private final Ignite ignite;

    /** */
    private final Vertx vertx;

    /**
     * @param ignite Ignite instance.
     * @param vertx Vertx instance.
     */
    public Bindings(Ignite ignite, Vertx vertx) {
        this.ignite = ignite;
        this.vertx = vertx;
    }

    /** {@inheritDoc} */
    @Override protected void configure() {
        bind(Ignite.class).toInstance(ignite);
        bind(Vertx.class).toInstance(vertx);

        bind(AuthProvider.class).to(IgniteAuth.class).in(Scopes.SINGLETON);
    }
}
