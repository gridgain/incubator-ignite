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

import org.apache.ignite.binary.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.internal.binary.*;
import org.apache.ignite.internal.binary.streams.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;

/**
 * Shared serialization/deserialization utils.
 */
class SerDes {
    /**
     * Restrict instantiation: static methods only.
     */
    private SerDes() {
    }

    /**
     * @param col Collection to serialize.
     * @param out Output stream.
     * @param elemWriter Collection element serializer
     */
    static <E> void write(Collection<E> col, BinaryOutputStream out, BiConsumer<BinaryOutputStream, E> elemWriter) {
        if (col == null || col.size() == 0)
            out.writeInt(0);
        else {
            out.writeInt(col.size());

            for (E e : col)
                elemWriter.accept(out, e);
        }
    }

    /**
     * @param in Input stream.
     * @param elemReader Collection element deserializer.
     * @return Deserialized collection.
     */
    static <E> Collection<E> read(BinaryInputStream in, Function<BinaryInputStream, E> elemReader) {
        Collection<E> col = new ArrayList<>();

        int cnt = in.readInt();

        for (int i = 0; i < cnt; i++)
            col.add(elemReader.apply(in));

        return col;
    }

    /** Serialize configuration to stream. */
    static void write(CacheClientConfiguration cfg, BinaryOutputStream out) {
        try (BinaryRawWriterEx writer = new BinaryWriterExImpl(null, out, null, null)) {

            int origPos = out.position();

            writer.writeInt(0); // configuration length is to be assigned in the end
            writer.writeShort((short)0); // properties count is to be assigned in the end

            AtomicInteger propCnt = new AtomicInteger(0);

            BiConsumer<ClientConfigItem, Consumer<BinaryRawWriter>> itemWriter = (cfgItem, cfgWriter) -> {
                writer.writeShort(cfgItem.code());

                cfgWriter.accept(writer);

                propCnt.incrementAndGet();
            };

            itemWriter.accept(ClientConfigItem.NAME, w -> w.writeString(cfg.getName()));
            itemWriter.accept(ClientConfigItem.CACHE_MODE, w -> w.writeInt(cfg.getCacheMode().ordinal()));
            itemWriter.accept(ClientConfigItem.ATOMICITY_MODE, w -> w.writeInt(cfg.getAtomicityMode().ordinal()));
            itemWriter.accept(ClientConfigItem.BACKUPS, w -> w.writeInt(cfg.getBackups()));
            itemWriter.accept(ClientConfigItem.WRITE_SYNC_MODE, w -> w.writeInt(cfg.getWriteSynchronizationMode().ordinal()));
            itemWriter.accept(ClientConfigItem.READ_FROM_BACKUP, w -> w.writeBoolean(cfg.isReadFromBackup()));
            itemWriter.accept(ClientConfigItem.EAGER_TTL, w -> w.writeBoolean(cfg.isEagerTtl()));
            itemWriter.accept(ClientConfigItem.GROUP_NAME, w -> w.writeString(cfg.getGroupName()));
            itemWriter.accept(ClientConfigItem.DEFAULT_LOCK_TIMEOUT, w -> w.writeLong(cfg.getDefaultLockTimeout()));
            itemWriter.accept(ClientConfigItem.PART_LOSS_POLICY, w -> w.writeInt(cfg.getPartitionLossPolicy().ordinal()));
            itemWriter.accept(ClientConfigItem.REBALANCE_BATCH_SIZE, w -> w.writeInt(cfg.getRebalanceBatchSize()));
            itemWriter.accept(ClientConfigItem.REBALANCE_BATCHES_PREFETCH_COUNT, w -> w.writeLong(cfg.getRebalanceBatchesPrefetchCount()));
            itemWriter.accept(ClientConfigItem.REBALANCE_DELAY, w -> w.writeLong(cfg.getRebalanceDelay()));
            itemWriter.accept(ClientConfigItem.REBALANCE_MODE, w -> w.writeInt(cfg.getRebalanceMode().ordinal()));
            itemWriter.accept(ClientConfigItem.REBALANCE_ORDER, w -> w.writeInt(cfg.getRebalanceOrder()));
            itemWriter.accept(ClientConfigItem.REBALANCE_THROTTLE, w -> w.writeLong(cfg.getRebalanceThrottle()));
            itemWriter.accept(ClientConfigItem.REBALANCE_TIMEOUT, w -> w.writeLong(cfg.getRebalanceTimeout()));
            itemWriter.accept(ClientConfigItem.COPY_ON_READ, w -> w.writeBoolean(cfg.isCopyOnRead()));
            itemWriter.accept(ClientConfigItem.DATA_REGION_NAME, w -> w.writeString(cfg.getDataRegionName()));
            itemWriter.accept(ClientConfigItem.STATS_ENABLED, w -> w.writeBoolean(cfg.isStatisticsEnabled()));
            itemWriter.accept(ClientConfigItem.MAX_ASYNC_OPS, w -> w.writeInt(cfg.getMaxConcurrentAsyncOperations()));
            itemWriter.accept(ClientConfigItem.MAX_QUERY_ITERATORS, w -> w.writeInt(cfg.getMaxQueryIteratorsCount()));
            itemWriter.accept(ClientConfigItem.ONHEAP_CACHE_ENABLED, w -> w.writeBoolean(cfg.isOnheapCacheEnabled()));
            itemWriter.accept(ClientConfigItem.QUERY_METRIC_SIZE, w -> w.writeInt(cfg.getQueryDetailMetricsSize()));
            itemWriter.accept(ClientConfigItem.QUERY_PARALLELISM, w -> w.writeInt(cfg.getQueryParallelism()));
            itemWriter.accept(ClientConfigItem.SQL_ESCAPE_ALL, w -> w.writeBoolean(cfg.isSqlEscapeAll()));
            itemWriter.accept(ClientConfigItem.SQL_IDX_MAX_INLINE_SIZE, w -> w.writeInt(cfg.getSqlIndexMaxInlineSize()));
            itemWriter.accept(ClientConfigItem.SQL_SCHEMA, w -> w.writeString(cfg.getSqlSchema()));
            itemWriter.accept(
                ClientConfigItem.KEY_CONFIGS,
                w -> SerDes.write(
                    cfg.getKeyConfiguration(),
                    out,
                    (unused, i) -> {
                        w.writeString(i.getTypeName());
                        w.writeString(i.getAffinityKeyFieldName());
                    }
                )
            );

            itemWriter.accept(
                ClientConfigItem.QUERY_ENTITIES,
                w -> SerDes.write(
                    cfg.getQueryEntities(),
                    out, (unused, e) -> {
                        w.writeString(e.getKeyType());
                        w.writeString(e.getValueType());
                        w.writeString(e.getTableName());
                        w.writeString(e.getKeyFieldName());
                        w.writeString(e.getValueFieldName());
                        SerDes.write(
                            e.getFields(),
                            out,
                            (unused2, f) -> {
                                w.writeString(f.getName());
                                w.writeString(f.getTypeName());
                                w.writeBoolean(f.isKeyField());
                                w.writeBoolean(f.isNotNull());
                                w.writeObject(f.getDefaultValue());
                            }
                        );
                        SerDes.write(
                            e.getAliases().entrySet(),
                            out, (unused3, a) -> {
                                w.writeString(a.getKey());
                                w.writeString(a.getValue());
                            }
                        );
                        SerDes.write(
                            e.getIndexes(),
                            out,
                            (unused4, i) -> {
                                w.writeString(i.getName());
                                w.writeByte((byte)i.getIndexType().ordinal());
                                w.writeInt(i.getInlineSize());
                                SerDes.write(i.getFields(), out, (unused5, f) -> {
                                        w.writeString(f.getName());
                                        w.writeBoolean(f.isDescending());
                                    }
                                );
                            });
                    }
                )
            );

            writer.writeInt(origPos, out.position() - origPos - 4); // configuration length
            writer.writeInt(origPos + 4, propCnt.get()); // properties count
        }
    }

    /** Deserialize configuration from stream. */
    static CacheClientConfiguration read(BinaryInputStream in) {
        CacheClientConfiguration cfg = new CacheClientConfiguration("TBD"); // cache name is to be assigned later

        BinaryRawReader reader = new BinaryReaderExImpl(IgniteBinaryMarshaller.INSTANCE.context(), in, null, true);

        reader.readInt(); // Do not need length to read data. The protocol defines fixed configuration layout.

        cfg.setAtomicityMode(CacheAtomicityMode.fromOrdinal(reader.readInt()));
        cfg.setBackups(reader.readInt());
        cfg.setCacheMode(CacheMode.fromOrdinal(reader.readInt()));
        cfg.setCopyOnRead(reader.readBoolean());
        cfg.setDataRegionName(reader.readString());
        cfg.setEagerTtl(reader.readBoolean());
        cfg.setStatisticsEnabled(reader.readBoolean());
        cfg.setGroupName(reader.readString());
        cfg.setDefaultLockTimeout(reader.readLong());
        cfg.setMaxConcurrentAsyncOperations(reader.readInt());
        cfg.setMaxQueryIteratorsCount(reader.readInt());
        cfg.setName(reader.readString());
        cfg.setOnheapCacheEnabled(reader.readBoolean());
        cfg.setPartitionLossPolicy(PartitionLossPolicy.fromOrdinal((byte)reader.readInt()));
        cfg.setQueryDetailMetricsSize(reader.readInt());
        cfg.setQueryParallelism(reader.readInt());
        cfg.setReadFromBackup(reader.readBoolean());
        cfg.setRebalanceBatchSize(reader.readInt());
        cfg.setRebalanceBatchesPrefetchCount(reader.readLong());
        cfg.setRebalanceDelay(reader.readLong());
        cfg.setRebalanceMode(CacheRebalanceMode.fromOrdinal(reader.readInt()));
        cfg.setRebalanceOrder(reader.readInt());
        cfg.setRebalanceThrottle(reader.readLong());
        cfg.setRebalanceTimeout(reader.readLong());
        cfg.setSqlEscapeAll(reader.readBoolean());
        cfg.setSqlIndexMaxInlineSize(reader.readInt());
        cfg.setSqlSchema(reader.readString());
        cfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.fromOrdinal(reader.readInt()));

        cfg.setKeyConfiguration(
            SerDes.read(in, unused -> new CacheKeyConfiguration(reader.readString(), reader.readString()))
        );

        cfg.setQueryEntities(SerDes.read(
            in,
            unused -> new QueryEntity(reader.readString(), reader.readString())
                .setTableName(reader.readString())
                .setKeyFieldName(reader.readString())
                .setValueFieldName(reader.readString())
                .setFields(SerDes.read(
                    in,
                    unused2 -> new QueryField(reader.readString(), reader.readString())
                        .setKeyField(reader.readBoolean())
                        .setNotNull(reader.readBoolean())
                        .setDefaultValue(reader.readObject())
                ))
                .setAliases(SerDes.read(
                    in,
                    unused3 -> new AbstractMap.SimpleEntry<>(reader.readString(), reader.readString())
                ).stream().collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue)))
                .setIndexes(SerDes.read(
                    in,
                    unused4 -> {
                        String name = reader.readString();
                        QueryIndexType type = QueryIndexType.fromOrdinal(reader.readByte());
                        int inlineSize = reader.readInt();

                        Collection<QueryIndexField> fields = SerDes.read(
                            in,
                            unused5 -> new QueryIndexField(reader.readString(), reader.readBoolean())
                        );

                        return new QueryIndex(fields, type).setName(name).setInlineSize(inlineSize);
                    }
                ))
        ));

        return cfg;
    }

    /** Serialize SQL field query to stream. */
    static void write(SqlFieldsQuery qry, BinaryOutputStream out) {
        writeObject(out, qry.getSchema());
        out.writeInt(qry.getPageSize());
        out.writeInt(-1); // do not limit
        writeObject(out, qry.getSql());
        SerDes.write(qry.getArgs() == null ? null : Arrays.asList(qry.getArgs()), out, SerDes::writeObject);
        out.writeByte((byte)0); // statement type ANY
        out.writeBoolean(qry.isDistributedJoins());
        out.writeBoolean(qry.isLocal());
        out.writeBoolean(qry.isReplicatedOnly());
        out.writeBoolean(qry.isEnforceJoinOrder());
        out.writeBoolean(qry.isCollocated());
        out.writeBoolean(qry.isLazy());
        out.writeLong(qry.getTimeout());
        out.writeBoolean(true); // include column names
    }

    /** Write Ignite binary object to output stream. */
    static void writeObject(BinaryOutputStream out, Object obj) {
        out.writeByteArray(IgniteBinaryMarshaller.INSTANCE.marshal(obj));
    }

    /** Read Ignite binary object from input stream. */
    @SuppressWarnings("unchecked")
    static <T> T readObject(BinaryInputStream in, boolean keepBinary) {
        Object val = IgniteBinaryMarshaller.INSTANCE.unmarshal(in);

        if (val instanceof BinaryObject && !keepBinary)
            val = ((BinaryObject)val).deserialize();

        return (T)val;
    }

    /** Thin client protocol cache configuration item codes. */
    private enum ClientConfigItem {
        /** Name. */NAME(0),
        /** Cache mode. */CACHE_MODE(1),
        /** Atomicity mode. */ATOMICITY_MODE(2),
        /** Backups. */BACKUPS(3),
        /** Write synchronization mode. */WRITE_SYNC_MODE(4),
        /** Read from backup. */READ_FROM_BACKUP(6),
        /** Eager ttl. */EAGER_TTL(405),
        /** Group name. */GROUP_NAME(400),
        /** Default lock timeout. */DEFAULT_LOCK_TIMEOUT(402),
        /** Partition loss policy. */PART_LOSS_POLICY(404),
        /** Rebalance batch size. */REBALANCE_BATCH_SIZE(303),
        /** Rebalance batches prefetch count. */REBALANCE_BATCHES_PREFETCH_COUNT(304),
        /** Rebalance delay. */REBALANCE_DELAY(301),
        /** Rebalance mode. */REBALANCE_MODE(300),
        /** Rebalance order. */REBALANCE_ORDER(305),
        /** Rebalance throttle. */REBALANCE_THROTTLE(306),
        /** Rebalance timeout. */REBALANCE_TIMEOUT(302),
        /** Copy on read. */COPY_ON_READ(5),
        /** Data region name. */DATA_REGION_NAME(100),
        /** Stats enabled. */STATS_ENABLED(406),
        /** Max async ops. */MAX_ASYNC_OPS(403),
        /** Max query iterators. */MAX_QUERY_ITERATORS(206),
        /** Onheap cache enabled. */ONHEAP_CACHE_ENABLED(101),
        /** Query metric size. */QUERY_METRIC_SIZE(202),
        /** Query parallelism. */QUERY_PARALLELISM(201),
        /** Sql escape all. */SQL_ESCAPE_ALL(205),
        /** Sql index max inline size. */SQL_IDX_MAX_INLINE_SIZE(204),
        /** Sql schema. */SQL_SCHEMA(203),
        /** Key configs. */KEY_CONFIGS(401),
        /** Key entities. */QUERY_ENTITIES(200);

        /** Code. */
        private final short code;

        /** */
        ClientConfigItem(int code) {
            this.code = (short)code;
        }

        /** @return Code. */
        short code() {
            return code;
        }
    }
}
