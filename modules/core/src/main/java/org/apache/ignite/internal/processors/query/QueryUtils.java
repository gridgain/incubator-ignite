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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheTypeMetadata;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;
import org.apache.ignite.cache.QueryIndexType;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheDefaultAffinityKeyMapper;
import org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl;
import org.apache.ignite.internal.processors.query.property.QueryBinaryProperty;
import org.apache.ignite.internal.processors.query.property.QueryClassProperty;
import org.apache.ignite.internal.processors.query.property.QueryFieldAccessor;
import org.apache.ignite.internal.processors.query.property.QueryMethodsAccessor;
import org.apache.ignite.internal.processors.query.property.QueryPropertyAccessor;
import org.apache.ignite.internal.processors.query.property.QueryReadOnlyMethodsAccessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Utility methods for queries.
 */
public class QueryUtils {
    /** */
    public static final String _KEY = "_key";

    /** */
    public static final String _VAL = "_val";

    /** */
    private static final Class<?> GEOMETRY_CLASS = U.classForName("com.vividsolutions.jts.geom.Geometry", null);

    /** */
    private static final Set<Class<?>> SQL_TYPES = new HashSet<>(F.<Class<?>>asList(
        Integer.class,
        Boolean.class,
        Byte.class,
        Short.class,
        Long.class,
        BigDecimal.class,
        Double.class,
        Float.class,
        Time.class,
        Timestamp.class,
        java.util.Date.class,
        java.sql.Date.class,
        String.class,
        UUID.class,
        byte[].class
    ));

    /**
     * Create type candidate for query entity.
     *
     * @param space Space.
     * @param cctx Cache context.
     * @param qryEntity Query entity.
     * @param mustDeserializeClss Classes which must be deserialized.
     * @return Type candidate.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryTypeCandidate typeForQueryEntity(String space, GridCacheContext cctx, QueryEntity qryEntity,
        List<Class<?>> mustDeserializeClss) throws IgniteCheckedException {
        if (F.isEmpty(qryEntity.findValueType()))
            throw new IgniteCheckedException("Value type is not set: " + qryEntity);

        GridKernalContext ctx = cctx.kernalContext();
        CacheConfiguration<?,?> ccfg = cctx.config();

        boolean binaryEnabled = ctx.cacheObjects().isBinaryEnabled(ccfg);

        CacheObjectContext coCtx = binaryEnabled ? ctx.cacheObjects().contextForCache(ccfg) : null;

        QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl();

        // Key and value classes still can be available if they are primitive or JDK part.
        // We need that to set correct types for _key and _val columns.
        Class<?> keyCls = U.classForName(qryEntity.findKeyType(), null);
        Class<?> valCls = U.classForName(qryEntity.findValueType(), null);

        // If local node has the classes and they are externalizable, we must use reflection properties.
        boolean keyMustDeserialize = mustDeserializeBinary(ctx, keyCls);
        boolean valMustDeserialize = mustDeserializeBinary(ctx, valCls);

        boolean keyOrValMustDeserialize = keyMustDeserialize || valMustDeserialize;

        if (keyCls == null)
            keyCls = Object.class;

        String simpleValType = ((valCls == null) ? typeName(qryEntity.findValueType()) : typeName(valCls));

        desc.name(simpleValType);

        desc.tableName(qryEntity.getTableName());

        if (binaryEnabled && !keyOrValMustDeserialize) {
            // Safe to check null.
            if (SQL_TYPES.contains(valCls))
                desc.valueClass(valCls);
            else
                desc.valueClass(Object.class);

            if (SQL_TYPES.contains(keyCls))
                desc.keyClass(keyCls);
            else
                desc.keyClass(Object.class);
        }
        else {
            if (valCls == null)
                throw new IgniteCheckedException("Failed to find value class in the node classpath " +
                    "(use default marshaller to enable binary objects) : " + qryEntity.findValueType());

            desc.valueClass(valCls);
            desc.keyClass(keyCls);
        }

        desc.keyTypeName(qryEntity.findKeyType());
        desc.valueTypeName(qryEntity.findValueType());

        desc.keyFieldName(qryEntity.getKeyFieldName());
        desc.valueFieldName(qryEntity.getValueFieldName());

        if (binaryEnabled && keyOrValMustDeserialize) {
            if (keyMustDeserialize)
                mustDeserializeClss.add(keyCls);

            if (valMustDeserialize)
                mustDeserializeClss.add(valCls);
        }

        QueryTypeIdKey typeId;
        QueryTypeIdKey altTypeId = null;

        if (valCls == null || (binaryEnabled && !keyOrValMustDeserialize)) {
            processBinaryMeta(ctx, qryEntity, desc);

            typeId = new QueryTypeIdKey(space, ctx.cacheObjects().typeId(qryEntity.findValueType()));

            if (valCls != null)
                altTypeId = new QueryTypeIdKey(space, valCls);

            if (!cctx.customAffinityMapper() && qryEntity.findKeyType() != null) {
                // Need to setup affinity key for distributed joins.
                String affField = ctx.cacheObjects().affinityField(qryEntity.findKeyType());

                if (affField != null)
                    desc.affinityKey(affField);
            }
        }
        else {
            processClassMeta(qryEntity, desc, coCtx);

            AffinityKeyMapper keyMapper = cctx.config().getAffinityMapper();

            if (keyMapper instanceof GridCacheDefaultAffinityKeyMapper) {
                String affField =
                    ((GridCacheDefaultAffinityKeyMapper)keyMapper).affinityKeyPropertyName(desc.keyClass());

                if (affField != null)
                    desc.affinityKey(affField);
            }

            typeId = new QueryTypeIdKey(space, valCls);
            altTypeId = new QueryTypeIdKey(space, ctx.cacheObjects().typeId(qryEntity.findValueType()));
        }

        return new QueryTypeCandidate(typeId, altTypeId, desc);
    }

    /**
     * Create type candidate for type metadata.
     *
     * @param space Space.
     * @param cctx Cache context.
     * @param meta Type metadata.
     * @param mustDeserializeClss Classes which must be deserialized.
     * @return Type candidate.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    @Nullable public static QueryTypeCandidate typeForCacheMetadata(String space, GridCacheContext cctx,
        CacheTypeMetadata meta, List<Class<?>> mustDeserializeClss) throws IgniteCheckedException {
        if (F.isEmpty(meta.getValueType()))
            throw new IgniteCheckedException("Value type is not set: " + meta);

        GridKernalContext ctx = cctx.kernalContext();
        CacheConfiguration<?,?> ccfg = cctx.config();

        boolean binaryEnabled = ctx.cacheObjects().isBinaryEnabled(ccfg);

        CacheObjectContext coCtx = binaryEnabled ? ctx.cacheObjects().contextForCache(ccfg) : null;

        if (meta.getQueryFields().isEmpty() && meta.getAscendingFields().isEmpty() &&
            meta.getDescendingFields().isEmpty() && meta.getGroups().isEmpty())
            return null;

        QueryTypeDescriptorImpl desc = new QueryTypeDescriptorImpl();

        // Key and value classes still can be available if they are primitive or JDK part.
        // We need that to set correct types for _key and _val columns.
        Class<?> keyCls = U.classForName(meta.getKeyType(), null);
        Class<?> valCls = U.classForName(meta.getValueType(), null);

        // If local node has the classes and they are externalizable, we must use reflection properties.
        boolean keyMustDeserialize = mustDeserializeBinary(ctx, keyCls);
        boolean valMustDeserialize = mustDeserializeBinary(ctx, valCls);

        boolean keyOrValMustDeserialize = keyMustDeserialize || valMustDeserialize;

        if (keyCls == null)
            keyCls = Object.class;

        String simpleValType = meta.getSimpleValueType();

        if (simpleValType == null)
            simpleValType = typeName(meta.getValueType());

        desc.name(simpleValType);

        if (binaryEnabled && !keyOrValMustDeserialize) {
            // Safe to check null.
            if (SQL_TYPES.contains(valCls))
                desc.valueClass(valCls);
            else
                desc.valueClass(Object.class);

            if (SQL_TYPES.contains(keyCls))
                desc.keyClass(keyCls);
            else
                desc.keyClass(Object.class);
        }
        else {
            desc.valueClass(valCls);
            desc.keyClass(keyCls);
        }

        desc.keyTypeName(meta.getKeyType());
        desc.valueTypeName(meta.getValueType());

        if (binaryEnabled && keyOrValMustDeserialize) {
            if (keyMustDeserialize)
                mustDeserializeClss.add(keyCls);

            if (valMustDeserialize)
                mustDeserializeClss.add(valCls);
        }

        QueryTypeIdKey typeId;
        QueryTypeIdKey altTypeId = null;

        if (valCls == null || (binaryEnabled && !keyOrValMustDeserialize)) {
            processBinaryMeta(ctx, meta, desc);

            typeId = new QueryTypeIdKey(space, ctx.cacheObjects().typeId(meta.getValueType()));

            if (valCls != null)
                altTypeId = new QueryTypeIdKey(space, valCls);
        }
        else {
            processClassMeta(meta, desc, coCtx);

            typeId = new QueryTypeIdKey(space, valCls);
            altTypeId = new QueryTypeIdKey(space, ctx.cacheObjects().typeId(meta.getValueType()));
        }

        return new QueryTypeCandidate(typeId, altTypeId, desc);
    }

    /**
     * Processes declarative metadata for class.
     *
     * @param meta Type metadata.
     * @param d Type descriptor.
     * @param coCtx Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    private static void processClassMeta(CacheTypeMetadata meta, QueryTypeDescriptorImpl d, CacheObjectContext coCtx)
        throws IgniteCheckedException {
        Map<String,String> aliases = meta.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        Class<?> keyCls = d.keyClass();
        Class<?> valCls = d.valueClass();

        assert keyCls != null;
        assert valCls != null;

        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet())
            addToIndex(d, keyCls, valCls, entry.getKey(), entry.getValue(), 0, IndexType.ASC, null, aliases, coCtx);

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet())
            addToIndex(d, keyCls, valCls, entry.getKey(), entry.getValue(), 0, IndexType.DESC, null, aliases, coCtx);

        for (String txtField : meta.getTextFields())
            addToIndex(d, keyCls, valCls, txtField, String.class, 0, IndexType.TEXT, null, aliases, coCtx);

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    Boolean descending = idxField.getValue().get2();

                    if (descending == null)
                        descending = false;

                    addToIndex(d, keyCls, valCls, idxField.getKey(), idxField.getValue().get1(), order,
                        descending ? IndexType.DESC : IndexType.ASC, idxName, aliases, coCtx);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            GridQueryProperty prop = buildProperty(
                keyCls,
                valCls,
                d.keyFieldName(),
                d.valueFieldName(),
                entry.getKey(),
                entry.getValue(),
                aliases,
                coCtx);

            d.addProperty(prop, false);
        }
    }

    /**
     * @param d Type descriptor.
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param idxOrder Order number in index or {@code -1} if no need to index.
     * @param idxType Index type.
     * @param idxName Index name.
     * @param aliases Aliases.
     * @throws IgniteCheckedException If failed.
     */
    private static void addToIndex(
        QueryTypeDescriptorImpl d,
        Class<?> keyCls,
        Class<?> valCls,
        String pathStr,
        Class<?> resType,
        int idxOrder,
        IndexType idxType,
        String idxName,
        Map<String,String> aliases,
        CacheObjectContext coCtx
    ) throws IgniteCheckedException {
        String propName;
        Class<?> propCls;

        if (_VAL.equals(pathStr)) {
            propName = _VAL;
            propCls = valCls;
        }
        else {
            GridQueryProperty prop = buildProperty(
                keyCls,
                valCls,
                d.keyFieldName(),
                d.valueFieldName(),
                pathStr,
                resType,
                aliases,
                coCtx);

            d.addProperty(prop, false);

            propName = prop.name();
            propCls = prop.type();
        }

        if (idxType != null) {
            if (idxName == null)
                idxName = propName + "_idx";

            if (idxOrder == 0) // Add index only on the first field.
                d.addIndex(idxName, isGeometryClass(propCls) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED, 0);

            if (idxType == IndexType.TEXT)
                d.addFieldToTextIndex(propName);
            else
                d.addFieldToIndex(idxName, propName, idxOrder, 0, idxType == IndexType.DESC);
        }
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param ctx Kernal context.
     * @param meta Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("deprecation")
    public static void processBinaryMeta(GridKernalContext ctx, CacheTypeMetadata meta, QueryTypeDescriptorImpl d)
        throws IgniteCheckedException {
        Map<String,String> aliases = meta.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        for (Map.Entry<String, Class<?>> entry : meta.getAscendingFields().entrySet()) {
            QueryBinaryProperty prop = buildBinaryProperty(ctx, entry.getKey(), entry.getValue(), aliases, null);

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED, 0);

            d.addFieldToIndex(idxName, prop.name(), 0, 0, false);
        }

        for (Map.Entry<String, Class<?>> entry : meta.getDescendingFields().entrySet()) {
            QueryBinaryProperty prop = buildBinaryProperty(ctx, entry.getKey(), entry.getValue(), aliases, null);

            d.addProperty(prop, false);

            String idxName = prop.name() + "_idx";

            d.addIndex(idxName, isGeometryClass(prop.type()) ? QueryIndexType.GEOSPATIAL : QueryIndexType.SORTED, 0);

            d.addFieldToIndex(idxName, prop.name(), 0, 0, true);
        }

        for (String txtIdx : meta.getTextFields()) {
            QueryBinaryProperty prop = buildBinaryProperty(ctx, txtIdx, String.class, aliases, null);

            d.addProperty(prop, false);

            d.addFieldToTextIndex(prop.name());
        }

        Map<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> grps = meta.getGroups();

        if (grps != null) {
            for (Map.Entry<String, LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>>> entry : grps.entrySet()) {
                String idxName = entry.getKey();

                LinkedHashMap<String, IgniteBiTuple<Class<?>, Boolean>> idxFields = entry.getValue();

                int order = 0;

                for (Map.Entry<String, IgniteBiTuple<Class<?>, Boolean>> idxField : idxFields.entrySet()) {
                    QueryBinaryProperty prop = buildBinaryProperty(ctx, idxField.getKey(), idxField.getValue().get1(),
                        aliases, null);

                    d.addProperty(prop, false);

                    Boolean descending = idxField.getValue().get2();

                    d.addFieldToIndex(idxName, prop.name(), order, 0, descending != null && descending);

                    order++;
                }
            }
        }

        for (Map.Entry<String, Class<?>> entry : meta.getQueryFields().entrySet()) {
            QueryBinaryProperty prop = buildBinaryProperty(ctx, entry.getKey(), entry.getValue(), aliases, null);

            if (!d.properties().containsKey(prop.name()))
                d.addProperty(prop, false);
        }
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param ctx Kernal context.
     * @param qryEntity Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public static void processBinaryMeta(GridKernalContext ctx, QueryEntity qryEntity, QueryTypeDescriptorImpl d)
        throws IgniteCheckedException {
        Map<String,String> aliases = qryEntity.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        Set<String> keyFields = qryEntity.getKeyFields();

        // We have to distinguish between empty and null keyFields when the key is not of SQL type -
        // when a key is not of SQL type, absence of a field in nonnull keyFields tell us that this field
        // is a value field, and null keyFields tells us that current configuration
        // does not tell us anything about this field's ownership.
        boolean hasKeyFields = (keyFields != null);

        boolean isKeyClsSqlType = isSqlType(d.keyClass());

        if (hasKeyFields && !isKeyClsSqlType) {
            //ensure that 'keyFields' is case sensitive subset of 'fields'
            for (String keyField : keyFields) {
                if (!qryEntity.getFields().containsKey(keyField))
                    throw new IgniteCheckedException("QueryEntity 'keyFields' property must be a subset of keys " +
                        "from 'fields' property (case sensitive): " + keyField);
            }
        }

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            Boolean isKeyField;

            if (isKeyClsSqlType) // We don't care about keyFields in this case - it might be null, or empty, or anything
                isKeyField = false;
            else
                isKeyField = (hasKeyFields ? keyFields.contains(entry.getKey()) : null);

            QueryBinaryProperty prop = buildBinaryProperty(ctx, entry.getKey(),
                U.classForName(entry.getValue(), Object.class, true), aliases, isKeyField);

            d.addProperty(prop, false);
        }

        processIndexes(qryEntity, d);
    }

    /**
     * Processes declarative metadata for binary object.
     *
     * @param qryEntity Declared metadata.
     * @param d Type descriptor.
     * @throws IgniteCheckedException If failed.
     */
    public static void processClassMeta(QueryEntity qryEntity, QueryTypeDescriptorImpl d, CacheObjectContext coCtx)
        throws IgniteCheckedException {
        Map<String,String> aliases = qryEntity.getAliases();

        if (aliases == null)
            aliases = Collections.emptyMap();

        for (Map.Entry<String, String> entry : qryEntity.getFields().entrySet()) {
            GridQueryProperty prop = buildProperty(
                d.keyClass(),
                d.valueClass(),
                d.keyFieldName(),
                d.valueFieldName(),
                entry.getKey(),
                U.classForName(entry.getValue(), Object.class),
                aliases,
                coCtx);

            d.addProperty(prop, false);
        }

        processIndexes(qryEntity, d);
    }

    /**
     * Processes indexes based on query entity.
     *
     * @param qryEntity Query entity to process.
     * @param d Type descriptor to populate.
     * @throws IgniteCheckedException If failed to build index information.
     */
    private static void processIndexes(QueryEntity qryEntity, QueryTypeDescriptorImpl d) throws IgniteCheckedException {
        if (!F.isEmpty(qryEntity.getIndexes())) {
            Map<String, String> aliases = qryEntity.getAliases();

            if (aliases == null)
                aliases = Collections.emptyMap();

            for (QueryIndex idx : qryEntity.getIndexes()) {
                String idxName = idx.getName();

                if (idxName == null)
                    idxName = QueryEntity.defaultIndexName(idx);

                QueryIndexType idxTyp = idx.getIndexType();

                if (idxTyp == QueryIndexType.SORTED || idxTyp == QueryIndexType.GEOSPATIAL) {
                    d.addIndex(idxName, idxTyp, idx.getInlineSize());

                    int i = 0;

                    for (Map.Entry<String, Boolean> entry : idx.getFields().entrySet()) {
                        String field = entry.getKey();
                        boolean asc = entry.getValue();

                        String alias = aliases.get(field);

                        if (alias != null)
                            field = alias;

                        d.addFieldToIndex(idxName, field, i++, idx.getInlineSize(), !asc);
                    }
                }
                else if (idxTyp == QueryIndexType.FULLTEXT){
                    for (String field : idx.getFields().keySet()) {
                        String alias = aliases.get(field);

                        if (alias != null)
                            field = alias;

                        d.addFieldToTextIndex(field);
                    }
                }
                else if (idxTyp != null)
                    throw new IllegalArgumentException("Unsupported index type [idx=" + idx.getName() +
                        ", typ=" + idxTyp + ']');
                else
                    throw new IllegalArgumentException("Index type is not set: " + idx.getName());
            }
        }

        //If there are some indexes configured for key or value alias columns,
        //we need to create similar indexes for original key and val columns.
        duplicateKeyValFieldsIndexes(qryEntity, d);

        //need to create index for key alias if it wasn't configured yet.
        if ((qryEntity.getKeyFieldName() != null) &&
                !hasSimilarIndex(d.indexes().values(), QueryIndexType.SORTED, F.asArray(qryEntity.getKeyFieldName()), new boolean[1])) {

            String idxName = makeUniqueName(d.indexes().keySet(), qryEntity.getKeyFieldName() + "_idx", false);
            d.addIndex(idxName, QueryIndexType.SORTED, -1);
            d.addFieldToIndex(idxName, qryEntity.getKeyFieldName(), 0,-1, false);
        }
    }

    /**
     * Create duplicates for indexes which contain key or value alias columns.
     * The duplicate indexes shall refer to _key and _val columns instead.
     *
     * @param qryEntity Query entity.
     * @param d Type descriptor.
     * @throws IgniteCheckedException
     */
    private static void duplicateKeyValFieldsIndexes(QueryEntity qryEntity, QueryTypeDescriptorImpl d) throws IgniteCheckedException {
        String keyFieldName = qryEntity.getKeyFieldName();
        String valueFieldName = qryEntity.getValueFieldName();

        if (F.isEmpty(keyFieldName) && F.isEmpty(valueFieldName))
            return;

        for (Map.Entry<String, GridQueryIndexDescriptor> e: d.indexes().entrySet()) {
            GridQueryIndexDescriptor idxDesc = e.getValue();
            String[] fields = idxDesc.fields().toArray(new String[idxDesc.fields().size()]);
            boolean[] descendings = new boolean[idxDesc.fields().size()];

            boolean found = false;
            for (int i = 0; i < fields.length; i++) {
                descendings[i] = idxDesc.descending(fields[i]);
                if (!F.isEmpty(keyFieldName) && fields[i].equals(keyFieldName)) {
                    fields[i] = _KEY;
                    found = true;
                }
                if (!F.isEmpty(valueFieldName) && fields[i].equals(valueFieldName)) {
                    fields[i] = _VAL;
                    found = true;
                }
            }

            if (found && !hasSimilarIndex(d.indexes().values(), idxDesc.type(), fields, descendings)) {
                String idxName = makeUniqueName(d.indexes().keySet(), e.getKey() + "_", true);
                d.addIndex(idxName, idxDesc.type(), idxDesc.inlineSize());
                for (int i = 0; i < fields.length; i++) {
                    if (idxDesc.type() == QueryIndexType.FULLTEXT)
                        d.addFieldToTextIndex(fields[i]);
                    else
                        d.addFieldToIndex(idxName, fields[i], i, idxDesc.inlineSize(), descendings[i]);
                }
            }
        }
    }

    /**
     * Make unique name with given prefix.
     *
     * @param names Set of existing names.
     * @param prefix Prefix to use for a name.
     * @param firstNumbered Whether first attempted name must have a number suffix.
     * @return Result.
     */
    private static String makeUniqueName(Set<String> names, String prefix, boolean firstNumbered) {
        int count = 2;
        String name = firstNumbered ? prefix + count++ : prefix;
        while (names.contains(name))
            name = prefix + count++;
        return name;
    }

    /**
     * Checks if collection of index descriptors contains any indexes
     * that are of the given type, field order and sorting.
     *
     * @param indexes Collection of index descriptors.
     * @param type Index type.
     * @param fields Field names.
     * @param descending Sorting order for each field name.
     * @return true if index with given properties exists.
     */
    private static boolean hasSimilarIndex(Collection<GridQueryIndexDescriptor> indexes, QueryIndexType type, String[] fields, boolean[] descending) {
        for (GridQueryIndexDescriptor d: indexes) {
            if (d.type() != type)
                continue;
            if (d.fields().size() != fields.length)
                continue;

            Iterator<String> a = d.fields().iterator();
            boolean diff = false;
            int i = 0;
            while (!diff && a.hasNext() && i < fields.length) {
                String aField = a.next();
                String bField = fields[i];
                diff = (!aField.equals(bField) || d.descending(aField) != descending[i]);
                i++;
            }

            if (!diff)
                return true;
        }
        return false;
    }

    /**
     * Builds binary object property.
     *
     * @param ctx Kernal context.
     * @param pathStr String representing path to the property. May contains dots '.' to identify
     *      nested fields.
     * @param resType Result type.
     * @param aliases Aliases.
     * @param isKeyField Key ownership flag, as defined in {@link QueryEntity#keyFields}: {@code true} if field belongs
     *      to key, {@code false} if it belongs to value, {@code null} if QueryEntity#keyFields is null.
     * @return Binary property.
     */
    public static QueryBinaryProperty buildBinaryProperty(GridKernalContext ctx, String pathStr, Class<?> resType,
                                     Map<String, String> aliases, @Nullable Boolean isKeyField) throws IgniteCheckedException {
        String[] path = pathStr.split("\\.");

        QueryBinaryProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            // The key flag that we've found out is valid for the whole path.
            res = new QueryBinaryProperty(ctx, prop, res, resType, isKeyField, alias);
        }

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param aliases Aliases.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    public static QueryClassProperty buildClassProperty(Class<?> keyCls, Class<?> valCls, String pathStr,
        Class<?> resType, Map<String,String> aliases, CacheObjectContext coCtx) throws IgniteCheckedException {
        QueryClassProperty res = buildClassProperty(
            true,
            keyCls,
            pathStr,
            resType,
            aliases,
            coCtx);

        if (res == null) // We check key before value consistently with BinaryProperty.
            res = buildClassProperty(false, valCls, pathStr, resType, aliases, coCtx);

        if (res == null)
            throw new IgniteCheckedException(propertyInitializationExceptionMessage(keyCls, valCls, pathStr, resType));

        return res;
    }

    /**
     * @param keyCls Key class.
     * @param valCls Value class.
     * @param keyFieldName Key Field.
     * @param valueFieldName Value Field.
     * @param pathStr Path string.
     * @param resType Result type.
     * @param aliases Aliases.
     * @return Class property.
     * @throws IgniteCheckedException If failed.
     */
    public static GridQueryProperty buildProperty(Class<?> keyCls, Class<?> valCls, String keyFieldName, String valueFieldName, String pathStr,
                                                  Class<?> resType, Map<String,String> aliases, CacheObjectContext coCtx) throws IgniteCheckedException {
        if (pathStr.equals(keyFieldName))
            return new KeyOrValProperty(true, pathStr, keyCls);

        if (pathStr.equals(valueFieldName))
            return new KeyOrValProperty(false, pathStr, valCls);

        return buildClassProperty(keyCls,
                valCls,
                pathStr,
                resType,
                aliases,
                coCtx);
    }

    /**
     * Exception message to compare in tests.
     *
     * @param keyCls key class
     * @param valCls value class
     * @param pathStr property name
     * @param resType property type
     * @return Exception message.
     */
    public static String propertyInitializationExceptionMessage(Class<?> keyCls, Class<?> valCls, String pathStr,
        Class<?> resType) {
        return "Failed to initialize property '" + pathStr + "' of type '" +
            resType.getName() + "' for key class '" + keyCls + "' and value class '" + valCls + "'. " +
            "Make sure that one of these classes contains respective getter method or field.";
    }

    /**
     * @param key If this is a key property.
     * @param cls Source type class.
     * @param pathStr String representing path to the property. May contains dots '.' to identify nested fields.
     * @param resType Expected result type.
     * @param aliases Aliases.
     * @return Property instance corresponding to the given path.
     */
    @SuppressWarnings("ConstantConditions")
    public static QueryClassProperty buildClassProperty(boolean key, Class<?> cls, String pathStr, Class<?> resType,
        Map<String,String> aliases, CacheObjectContext coCtx) {
        String[] path = pathStr.split("\\.");

        QueryClassProperty res = null;

        StringBuilder fullName = new StringBuilder();

        for (String prop : path) {
            if (fullName.length() != 0)
                fullName.append('.');

            fullName.append(prop);

            String alias = aliases.get(fullName.toString());

            QueryPropertyAccessor accessor = findProperty(prop, cls);

            if (accessor == null)
                return null;

            QueryClassProperty tmp = new QueryClassProperty(accessor, key, alias, coCtx);

            tmp.parent(res);

            cls = tmp.type();

            res = tmp;
        }

        if (!U.box(resType).isAssignableFrom(U.box(res.type())))
            return null;

        return res;
    }

    /**
     * Find a member (either a getter method or a field) with given name of given class.
     * @param prop Property name.
     * @param cls Class to search for a member in.
     * @return Member for given name.
     */
    @Nullable private static QueryPropertyAccessor findProperty(String prop, Class<?> cls) {
        StringBuilder getBldr = new StringBuilder("get");
        getBldr.append(prop);
        getBldr.setCharAt(3, Character.toUpperCase(getBldr.charAt(3)));

        StringBuilder setBldr = new StringBuilder("set");
        setBldr.append(prop);
        setBldr.setCharAt(3, Character.toUpperCase(setBldr.charAt(3)));

        try {
            Method getter = cls.getMethod(getBldr.toString());

            Method setter;

            try {
                // Setter has to have the same name like 'setXxx' and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(setBldr.toString(), getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignore) {
            // No-op.
        }

        getBldr = new StringBuilder("is");
        getBldr.append(prop);
        getBldr.setCharAt(2, Character.toUpperCase(getBldr.charAt(2)));

        // We do nothing about setBldr here as it corresponds to setProperty name which is what we need
        // for boolean property setter as well
        try {
            Method getter = cls.getMethod(getBldr.toString());

            Method setter;

            try {
                // Setter has to have the same name like 'setXxx' and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(setBldr.toString(), getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignore) {
            // No-op.
        }

        Class cls0 = cls;

        while (cls0 != null)
            try {
                return new QueryFieldAccessor(cls0.getDeclaredField(prop));
            }
            catch (NoSuchFieldException ignored) {
                cls0 = cls0.getSuperclass();
            }

        try {
            Method getter = cls.getMethod(prop);

            Method setter;

            try {
                // Setter has to have the same name and single param of the same type
                // as the return type of the getter.
                setter = cls.getMethod(prop, getter.getReturnType());
            }
            catch (NoSuchMethodException ignore) {
                // Have getter, but no setter - return read-only accessor.
                return new QueryReadOnlyMethodsAccessor(getter, prop);
            }

            return new QueryMethodsAccessor(getter, setter, prop);
        }
        catch (NoSuchMethodException ignored) {
            // No-op.
        }

        // No luck.
        return null;
    }

    /**
     * Check whether type still must be deserialized when binary marshaller is set.
     *
     * @param ctx Kernal context.
     * @param cls Class.
     * @return {@code True} if will be deserialized.
     */
    private static boolean mustDeserializeBinary(GridKernalContext ctx, Class cls) {
        if (cls != null && ctx.config().getMarshaller() instanceof BinaryMarshaller) {
            CacheObjectBinaryProcessorImpl proc0 = (CacheObjectBinaryProcessorImpl)ctx.cacheObjects();

            return proc0.binaryContext().mustDeserialize(cls);
        }
        else
            return false;
    }

    /**
     * Checks if the given class can be mapped to a simple SQL type.
     *
     * @param cls Class.
     * @return {@code true} If can.
     */
    public static boolean isSqlType(Class<?> cls) {
        cls = U.box(cls);

        return SQL_TYPES.contains(cls) || QueryUtils.isGeometryClass(cls);
    }

    /**
     * Checks if the given class is GEOMETRY.
     *
     * @param cls Class.
     * @return {@code true} If this is geometry.
     */
    public static boolean isGeometryClass(Class<?> cls) {
        return GEOMETRY_CLASS != null && GEOMETRY_CLASS.isAssignableFrom(cls);
    }

    /**
     * Gets type name by class.
     *
     * @param clsName Class name.
     * @return Type name.
     */
    public static String typeName(String clsName) {
        int pkgEnd = clsName.lastIndexOf('.');

        if (pkgEnd >= 0 && pkgEnd < clsName.length() - 1)
            clsName = clsName.substring(pkgEnd + 1);

        if (clsName.endsWith("[]"))
            clsName = clsName.substring(0, clsName.length() - 2) + "_array";

        int parentEnd = clsName.lastIndexOf('$');

        if (parentEnd >= 0)
            clsName = clsName.substring(parentEnd + 1);

        return clsName;
    }

    /**
     * Gets type name by class.
     *
     * @param cls Class.
     * @return Type name.
     */
    public static String typeName(Class<?> cls) {
        String typeName = cls.getSimpleName();

        // To protect from failure on anonymous classes.
        if (F.isEmpty(typeName)) {
            String pkg = cls.getPackage().getName();

            typeName = cls.getName().substring(pkg.length() + (pkg.isEmpty() ? 0 : 1));
        }

        if (cls.isArray()) {
            assert typeName.endsWith("[]");

            typeName = typeName.substring(0, typeName.length() - 2) + "_array";
        }

        return typeName;
    }

    /**
     * @param timeout Timeout.
     * @param timeUnit Time unit.
     * @return Converted time.
     */
    public static int validateTimeout(int timeout, TimeUnit timeUnit) {
        A.ensure(timeUnit != TimeUnit.MICROSECONDS && timeUnit != TimeUnit.NANOSECONDS,
            "timeUnit minimal resolution is millisecond.");

        A.ensure(timeout >= 0, "timeout value should be non-negative.");

        long tmp = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);

        return (int) tmp;
    }

    /**
     * @param ccfg Cache configuration.
     * @return {@code true} If query index must be enabled for this cache.
     */
    public static boolean isEnabled(CacheConfiguration<?,?> ccfg) {
        return !F.isEmpty(ccfg.getIndexedTypes()) ||
            !F.isEmpty(ccfg.getTypeMetadata()) ||
            !F.isEmpty(ccfg.getQueryEntities());
    }

    /**
     * Private constructor.
     */
    private QueryUtils() {
        // No-op.
    }
    /**
     * The way to index.
     */
    private enum IndexType {
        /** Ascending index. */
        ASC,

        /** Descending index. */
        DESC,

        /** Text index. */
        TEXT
    }

    /** Property used for keyFieldName or valueFieldName */
    public static class KeyOrValProperty implements GridQueryProperty {
        /** */
        boolean isKey;

        /** */
        String name;

        /** */
        Class<?> cls;

        /** */
        public KeyOrValProperty(boolean key, String name, Class<?> cls) {
            this.isKey = key;
            this.name = name;
            this.cls = cls;
        }

        /** {@inheritDoc} */
        @Override public Object value(Object key, Object val) throws IgniteCheckedException {
            return isKey ? key : val;
        }

        /** {@inheritDoc} */
        @Override public void setValue(Object key, Object val, Object propVal) throws IgniteCheckedException {
            //No-op
        }

        /** {@inheritDoc} */
        @Override public String name() {
            return name;
        }

        /** {@inheritDoc} */
        @Override public Class<?> type() {
            return cls;
        }

        /** {@inheritDoc} */
        @Override public boolean key() {
            return isKey;
        }

        /** {@inheritDoc} */
        @Override public GridQueryProperty parent() {
            return null;
        }
    }
}
