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

package org.apache.ignite.client;

import java.util.function.*;

import org.apache.ignite.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.binary.*;
import org.apache.ignite.internal.binary.streams.*;

import java.util.*;

/**
 * Implementation of {@link IgniteClient} over TCP protocol.
 */
class TcpIgniteClient implements IgniteClient {
    /** Channel. */
    private final ReliableChannel ch;

    /** Ignite Binary. */
    private final IgniteBinary binary;

    /**
     * Private constructor. Use {@link IgniteClient#start(IgniteClientConfiguration)} to create an instance of
     * {@link TcpClientChannel}.
     */
    private TcpIgniteClient(IgniteClientConfiguration cfg) {
        IgniteBinaryMarshaller.INSTANCE.setBinaryConfiguration(cfg.getBinaryConfiguration());

        binary = new ClientBinary();

        Function<ClientChannelConfiguration, Result<ClientChannel>> chFactory = chCfg -> {
            try {
                return new Result<>(new TcpClientChannel(chCfg));
            }
            catch (IgniteClientException e) {
                return new Result<>(e);
            }
        };

        this.ch = new ReliableChannel(chFactory, cfg);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ch.close();
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheClient<K, V> getOrCreateCache(String name) throws IgniteClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_GET_OR_CREATE_WITH_NAME, req -> req.writeByteArray(marshalString(name)));

        return new TcpCacheClient<>(name, ch);
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheClient<K, V> getOrCreateCache(
        CacheClientConfiguration cfg) throws IgniteClientException {
        ensureCacheConfiguration(cfg);

        ch.request(ClientOperation.CACHE_GET_OR_CREATE_WITH_CONFIGURATION, req -> SerDes.write(cfg, req));

        return new TcpCacheClient<>(cfg.getName(), ch);
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheClient<K, V> cache(String name) {
        ensureCacheName(name);

        return new TcpCacheClient<>(name, ch);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> cacheNames() throws IgniteClientException {
        return ch.service(ClientOperation.CACHE_GET_NAMES, res -> Arrays.asList(BinaryUtils.doReadStringArray(res)));
    }

    /** {@inheritDoc} */
    @Override public void destroyCache(String name) throws IgniteClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_DESTROY, req -> req.writeInt(CacheClient.cacheId(name)));
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheClient<K, V> createCache(String name) throws IgniteClientException {
        ensureCacheName(name);

        ch.request(ClientOperation.CACHE_CREATE_WITH_NAME, req -> req.writeByteArray(marshalString(name)));

        return new TcpCacheClient<>(name, ch);
    }

    /** {@inheritDoc} */
    @Override public <K, V> CacheClient<K, V> createCache(CacheClientConfiguration cfg) throws IgniteClientException {
        ensureCacheConfiguration(cfg);

        ch.request(ClientOperation.CACHE_CREATE_WITH_CONFIGURATION, req -> SerDes.write(cfg, req));

        return new TcpCacheClient<>(cfg.getName(), ch);
    }

    /** {@inheritDoc} */
    @Override public IgniteBinary binary() {
        return binary;
    }

    /** {@inheritDoc} */
    @Override public FieldsQueryCursor<List<?>> query(SqlFieldsQuery qry) {
        if (qry == null)
            throw new NullPointerException("qry");

        Consumer<BinaryOutputStream> qryWriter = out -> {
            out.writeInt(0); // no cache ID
            out.writeByte((byte)1); // keep binary
            SerDes.write(qry, out);
        };

        return new ClientFieldsQueryCursor<>(new ClientFieldsQueryPager(
            ch,
            ClientOperation.QUERY_SQL_FIELDS,
            ClientOperation.QUERY_SQL_FIELDS_CURSOR_GET_PAGE,
            qryWriter,
            true
        ));
    }

    /** {@inheritDoc} */
    @Override public int nonQuery(SqlFieldsQuery qry) {
        return query(qry).getAll().size();
    }

    /**
     * Initializes new instance of {@link IgniteClient}.
     * <p>
     * Server connection will be lazily initialized when first required.
     *
     * @param cfg Thin client configuration.
     * @return Successfully opened thin client connection.
     */
    static IgniteClient start(IgniteClientConfiguration cfg) {
        return new TcpIgniteClient(cfg);
    }

    /** @throws IllegalArgumentException if the specified cache name is invalid. */
    private static void ensureCacheName(String name) {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Cache name must be specified");
    }

    /** @throws IllegalArgumentException if the specified cache name is invalid. */
    private static void ensureCacheConfiguration(CacheClientConfiguration cfg) {
        if (cfg == null)
            throw new IllegalArgumentException("Cache configuration must be specified");

        ensureCacheName(cfg.getName());
    }

    /** Serialize String for thin client protocol. */
    private static byte[] marshalString(String s) {
        try (BinaryOutputStream out = new BinaryHeapOutputStream(s == null ? 1 : s.length() + 20);
             BinaryRawWriterEx writer = new BinaryWriterExImpl(null, out, null, null)
        ) {
            writer.writeString(s);

            return out.arrayCopy();
        }
    }
}
