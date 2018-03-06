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

package org.apache.ignite.internal.thinclient;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.binary.BinaryRawWriter;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheKeyConfiguration;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.streams.BinaryInputStream;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;
import org.apache.ignite.configuration.ClientCacheConfiguration;

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
        Collection<E> col = new LinkedList<>(); // needs to be ordered for some use cases

        int cnt = in.readInt();

        for (int i = 0; i < cnt; i++)
            col.add(elemReader.apply(in));

        return col;
    }

    /** Serialize configuration to stream. */
    static void write(ClientCacheConfiguration cfg, BinaryOutputStream out) {
        try (BinaryRawWriterEx writer =
                 new BinaryWriterExImpl(IgniteBinaryMarshaller.INSTANCE.context(), out, null, null)
        ) {
            int origPos = out.position();

            writer.writeInt(0); // configuration length is to be assigned in the end
            writer.writeShort((short)0); // properties count is to be assigned in the end

            AtomicInteger propCnt = new AtomicInteger(0);

            BiConsumer<CfgItem, Consumer<BinaryRawWriter>> itemWriter = (cfgItem, cfgWriter) -> {
                writer.writeShort(cfgItem.code());

                cfgWriter.accept(writer);

                propCnt.incrementAndGet();
            };

            itemWriter.accept(CfgItem.NAME, w -> w.writeString(cfg.getName()));
            itemWriter.accept(CfgItem.CACHE_MODE, w -> w.writeInt(cfg.getCacheMode().ordinal()));
            itemWriter.accept(CfgItem.ATOMICITY_MODE, w -> w.writeInt(cfg.getAtomicityMode().ordinal()));
            itemWriter.accept(CfgItem.BACKUPS, w -> w.writeInt(cfg.getBackups()));
            itemWriter.accept(CfgItem.WRITE_SYNC_MODE, w -> w.writeInt(cfg.getWriteSynchronizationMode().ordinal()));
            itemWriter.accept(CfgItem.READ_FROM_BACKUP, w -> w.writeBoolean(cfg.isReadFromBackup()));
            itemWriter.accept(CfgItem.EAGER_TTL, w -> w.writeBoolean(cfg.isEagerTtl()));
            itemWriter.accept(CfgItem.GROUP_NAME, w -> w.writeString(cfg.getGroupName()));
            itemWriter.accept(CfgItem.DEFAULT_LOCK_TIMEOUT, w -> w.writeLong(cfg.getDefaultLockTimeout()));
            itemWriter.accept(CfgItem.PART_LOSS_POLICY, w -> w.writeInt(cfg.getPartitionLossPolicy().ordinal()));
            itemWriter.accept(CfgItem.REBALANCE_BATCH_SIZE, w -> w.writeInt(cfg.getRebalanceBatchSize()));
            itemWriter.accept(CfgItem.REBALANCE_BATCHES_PREFETCH_COUNT, w -> w.writeLong(cfg.getRebalanceBatchesPrefetchCount()));
            itemWriter.accept(CfgItem.REBALANCE_DELAY, w -> w.writeLong(cfg.getRebalanceDelay()));
            itemWriter.accept(CfgItem.REBALANCE_MODE, w -> w.writeInt(cfg.getRebalanceMode().ordinal()));
            itemWriter.accept(CfgItem.REBALANCE_ORDER, w -> w.writeInt(cfg.getRebalanceOrder()));
            itemWriter.accept(CfgItem.REBALANCE_THROTTLE, w -> w.writeLong(cfg.getRebalanceThrottle()));
            itemWriter.accept(CfgItem.REBALANCE_TIMEOUT, w -> w.writeLong(cfg.getRebalanceTimeout()));
            itemWriter.accept(CfgItem.COPY_ON_READ, w -> w.writeBoolean(cfg.isCopyOnRead()));
            itemWriter.accept(CfgItem.DATA_REGION_NAME, w -> w.writeString(cfg.getDataRegionName()));
            itemWriter.accept(CfgItem.STATS_ENABLED, w -> w.writeBoolean(cfg.isStatisticsEnabled()));
            itemWriter.accept(CfgItem.MAX_ASYNC_OPS, w -> w.writeInt(cfg.getMaxConcurrentAsyncOperations()));
            itemWriter.accept(CfgItem.MAX_QUERY_ITERATORS, w -> w.writeInt(cfg.getMaxQueryIteratorsCount()));
            itemWriter.accept(CfgItem.ONHEAP_CACHE_ENABLED, w -> w.writeBoolean(cfg.isOnheapCacheEnabled()));
            itemWriter.accept(CfgItem.QUERY_METRIC_SIZE, w -> w.writeInt(cfg.getQueryDetailMetricsSize()));
            itemWriter.accept(CfgItem.QUERY_PARALLELISM, w -> w.writeInt(cfg.getQueryParallelism()));
            itemWriter.accept(CfgItem.SQL_ESCAPE_ALL, w -> w.writeBoolean(cfg.isSqlEscapeAll()));
            itemWriter.accept(CfgItem.SQL_IDX_MAX_INLINE_SIZE, w -> w.writeInt(cfg.getSqlIndexMaxInlineSize()));
            itemWriter.accept(CfgItem.SQL_SCHEMA, w -> w.writeString(cfg.getSqlSchema()));
            itemWriter.accept(
                CfgItem.KEY_CONFIGS,
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
                CfgItem.QUERY_ENTITIES,
                w -> SerDes.write(
                    cfg.getQueryEntities(),
                    out, (unused, e) -> {
                        w.writeString(e.getKeyType());
                        w.writeString(e.getValueType());
                        w.writeString(e.getTableName());
                        w.writeString(e.getKeyFieldName());
                        w.writeString(e.getValueFieldName());
                        SerDes.write(
                            e.getFields().entrySet(),
                            out,
                            (unused2, f) -> {
                                QueryField qf = new QueryField(e, f);

                                w.writeString(qf.getName());
                                w.writeString(qf.getTypeName());
                                w.writeBoolean(qf.isKey());
                                w.writeBoolean(qf.isNotNull());
                                w.writeObject(qf.getDefaultValue());
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
                                SerDes.write(i.getFields().entrySet(), out, (unused5, f) -> {
                                        w.writeString(f.getKey());
                                        w.writeBoolean(f.getValue());
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
    static ClientCacheConfiguration read(BinaryInputStream in) {
        BinaryRawReader reader = new BinaryReaderExImpl(IgniteBinaryMarshaller.INSTANCE.context(), in, null, true);

        reader.readInt(); // Do not need length to read data. The protocol defines fixed configuration layout.

        return new ClientCacheConfiguration().setName("TBD") // cache name is to be assigned later
            .setAtomicityMode(CacheAtomicityMode.fromOrdinal(reader.readInt()))
            .setBackups(reader.readInt())
            .setCacheMode(CacheMode.fromOrdinal(reader.readInt()))
            .setCopyOnRead(reader.readBoolean())
            .setDataRegionName(reader.readString())
            .setEagerTtl(reader.readBoolean())
            .setStatisticsEnabled(reader.readBoolean())
            .setGroupName(reader.readString())
            .setDefaultLockTimeout(reader.readLong())
            .setMaxConcurrentAsyncOperations(reader.readInt())
            .setMaxQueryIteratorsCount(reader.readInt())
            .setName(reader.readString())
            .setOnheapCacheEnabled(reader.readBoolean())
            .setPartitionLossPolicy(PartitionLossPolicy.fromOrdinal((byte)reader.readInt()))
            .setQueryDetailMetricsSize(reader.readInt())
            .setQueryParallelism(reader.readInt())
            .setReadFromBackup(reader.readBoolean())
            .setRebalanceBatchSize(reader.readInt())
            .setRebalanceBatchesPrefetchCount(reader.readLong())
            .setRebalanceDelay(reader.readLong())
            .setRebalanceMode(CacheRebalanceMode.fromOrdinal(reader.readInt()))
            .setRebalanceOrder(reader.readInt())
            .setRebalanceThrottle(reader.readLong())
            .setRebalanceTimeout(reader.readLong())
            .setSqlEscapeAll(reader.readBoolean())
            .setSqlIndexMaxInlineSize(reader.readInt())
            .setSqlSchema(reader.readString())
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.fromOrdinal(reader.readInt()))
            .setKeyConfiguration(
                SerDes.read(in, unused -> new CacheKeyConfiguration(reader.readString(), reader.readString()))
                    .toArray(new CacheKeyConfiguration[0])
            ).setQueryEntities(SerDes.read(
                in,
                unused -> {
                    QueryEntity qryEntity = new QueryEntity(reader.readString(), reader.readString())
                        .setTableName(reader.readString())
                        .setKeyFieldName(reader.readString())
                        .setValueFieldName(reader.readString());

                    Collection<QueryField> qryFields = SerDes.read(
                        in,
                        unused2 -> new QueryField(
                            reader.readString(),
                            reader.readString(),
                            reader.readBoolean(),
                            reader.readBoolean(),
                            reader.readObject()
                        )
                    );

                    return qryEntity
                        .setFields(qryFields.stream().collect(Collectors.toMap(
                            QueryField::getName, QueryField::getTypeName, (a, b) -> a, LinkedHashMap::new
                        )))
                        .setKeyFields(qryFields.stream()
                            .filter(QueryField::isKey)
                            .map(QueryField::getName)
                            .collect(Collectors.toCollection(LinkedHashSet::new))
                        )
                        .setNotNullFields(qryFields.stream()
                            .filter(QueryField::isNotNull)
                            .map(QueryField::getName)
                            .collect(Collectors.toSet())
                        )
                        .setDefaultFieldValues(qryFields.stream()
                            .filter(f -> f.getDefaultValue() != null)
                            .collect(Collectors.toMap(QueryField::getName, QueryField::getDefaultValue))
                        )
                        .setAliases(SerDes.read(
                            in,
                            unused3 -> new SimpleEntry<>(reader.readString(), reader.readString())
                        ).stream().collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)))
                        .setIndexes(SerDes.read(
                            in,
                            unused4 -> {
                                String name = reader.readString();
                                QueryIndexType type = QueryIndexType.fromOrdinal(reader.readByte());
                                int inlineSize = reader.readInt();

                                LinkedHashMap<String, Boolean> fields = SerDes.read(
                                    in,
                                    unused5 -> new SimpleEntry<>(reader.readString(), reader.readBoolean())
                                ).stream().collect(Collectors.toMap(
                                    SimpleEntry::getKey,
                                    SimpleEntry::getValue,
                                    (a, b) -> a,
                                    LinkedHashMap::new
                                ));

                                return new QueryIndex(fields, type).setName(name).setInlineSize(inlineSize);
                            }
                        ));
                }
            ).toArray(new QueryEntity[0]));
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

    /**
     * Get cache ID by cache name.
     */
    static int cacheId(String name) {
        Objects.requireNonNull(name, "name");

        return name.hashCode();
    }

    /** Thin client protocol cache configuration item codes. */
    private enum CfgItem {
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
        CfgItem(int code) {
            this.code = (short)code;
        }

        /** @return Code. */
        short code() {
            return code;
        }
    }

    /** A helper class to translate query fields. */
    private static final class QueryField {
        /** Name. */
        private final String name;

        /** Type name. */
        private final String typeName;

        /** Is key. */
        private final boolean isKey;

        /** Is not null. */
        private final boolean isNotNull;

        /** Default value. */
        private final Object dfltVal;

        /** Serialization constructor. */
        QueryField(QueryEntity e, Map.Entry<String, String> nameAndTypeName) {
            name = nameAndTypeName.getKey();
            typeName = nameAndTypeName.getValue();

            Set<String> keys = e.getKeyFields();
            Set<String> notNulls = e.getNotNullFields();
            Map<String, Object> dflts = e.getDefaultFieldValues();

            isKey = keys != null && keys.contains(name);
            isNotNull = notNulls != null && notNulls.contains(name);
            dfltVal = dflts == null ? null : dflts.get(name);
        }

        /** Deserialization constructor. */
        public QueryField(String name, String typeName, boolean isKey, boolean isNotNull, Object dfltVal) {
            this.name = name;
            this.typeName = typeName;
            this.isKey = isKey;
            this.isNotNull = isNotNull;
            this.dfltVal = dfltVal;
        }

        /**
         * @return Name.
         */
        String getName() {
            return name;
        }

        /**
         * @return Type name.
         */
        String getTypeName() {
            return typeName;
        }

        /**
         * @return Is Key.
         */
        boolean isKey() {
            return isKey;
        }

        /**
         * @return Is Not Null.
         */
        boolean isNotNull() {
            return isNotNull;
        }

        /**
         * @return Default value.
         */
        Object getDefaultValue() {
            return dfltVal;
        }
    }
}
