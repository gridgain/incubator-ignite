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

package org.apache.ignite.console.config;

import java.io.File;
import java.net.URL;
import java.util.function.Function;
import io.vertx.config.ConfigChange;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.ReadStream;
import org.apache.ignite.Ignition;

/**
 * Configuration helper.
 */
public class WebConsoleConfigurationHelper {
    /**
     * Config format.
     */
    public enum Format {
        /** */
        JSON,

        /** */
        PROPERTIES,

        /** */
        XML
    }

    /**
     * @param vertx Vertx.
     * @param fmt File format: JSON or properties.
     * @param path Path to configuration file.
     * @param beanName Optional, bean name, required only for XML configuration.
     * @return Web Console configuration receiver.
     */
    public static ConfigRetriever loadConfiguration(Vertx vertx, Format fmt, String path, String beanName) {
        if (fmt == Format.XML)
            return new XmlConfigRetriever(path, beanName);

        ConfigStoreOptions storeOpts = new ConfigStoreOptions()
            .setType("file")
            .setFormat(fmt.name().toLowerCase())
            .setConfig(new JsonObject().put("path", path));

        return ConfigRetriever.create(vertx, new ConfigRetrieverOptions().addStore(storeOpts));
    }

    /**
     * Simple XML configuration receiver.
     */
    private static class XmlConfigRetriever implements ConfigRetriever {
        /***/
        private final String path;

        /**
         *
         */
        private final String beanName;

        /**
         * @param path Path to configuration file.
         * @param beanName Bean name to load.
         */
        XmlConfigRetriever(String path, String beanName) {
            this.path = path;
            this.beanName = beanName;
        }

        /** {@inheritDoc} */
        @Override public void getConfig(Handler<AsyncResult<JsonObject>> hnd) {
            Future<JsonObject> fut = Future.future();

            try {
                URL url = new File(path).toURI().toURL();

                JsonObject json = JsonObject.mapFrom(Ignition.loadSpringBean(url, beanName));

                fut.complete(json);
            }
            catch (Throwable e) {
                fut.fail(e);
            }

            hnd.handle(fut);
        }

        /** {@inheritDoc} */
        @Override public void close() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public JsonObject getCachedConfig() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void listen(Handler<ConfigChange> lsnr) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public ConfigRetriever setBeforeScanHandler(Handler<Void> hnd) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public ConfigRetriever setConfigurationProcessor(Function<JsonObject, JsonObject> processor) {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public ReadStream<JsonObject> configStream() {
            throw new UnsupportedOperationException();
        }
    }
}
