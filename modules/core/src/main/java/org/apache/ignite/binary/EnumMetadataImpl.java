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

package org.apache.ignite.binary;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.util.typedef.F;

import java.util.LinkedHashMap;
import java.util.Map;

/** Enum metadata implementation */
public class EnumMetadataImpl implements EnumMetadata {
    /** */
    private String typeName;

    /** */
    private Map<String, Integer> nameToOrdinal;

    /** */
    private Map<Integer, String> ordinalToNames;

    /**
     * Metadata constructor.
     *
     * @param cls Enum class.
     */
    public EnumMetadataImpl(Class<?> cls) {
        assert cls.isEnum();
        this.typeName = cls.getName();

        Object[] constants = cls.getEnumConstants();
        nameToOrdinal = new LinkedHashMap<>(constants.length);
        ordinalToNames = new LinkedHashMap<>(constants.length);

        for (Object o: constants) {
            Enum e = (Enum)o;
            ordinalToNames.put(e.ordinal(), e.name());
            nameToOrdinal.put(e.name().toUpperCase(), e.ordinal());
        }
    }

    /**
     * Metadata constructor.
     *
     * @param typeName Type name.
     * @param map Enum constants mapping.
     */
    public EnumMetadataImpl(String typeName, Map<Integer, String> map) {
        this.typeName = typeName;
        nameToOrdinal = new LinkedHashMap<>(map.size());
        ordinalToNames = new LinkedHashMap<>(map.size());
        for (Map.Entry<Integer, String> e: map.entrySet()) {
            nameToOrdinal.put(e.getValue(), e.getKey());
            ordinalToNames.put(e.getKey(), e.getValue());
        }
    }

    /**
     * Metadata constructor.
     *
     * @param typeName Type name.
     * @param namesToOrd Mapping of names to ordinal values.
     * @param names Mapping of ordinal values to names.
     */
    public EnumMetadataImpl(String typeName, Map<String, Integer> namesToOrd, Map<Integer, String> names) {
        this.typeName = typeName;
        this.nameToOrdinal = namesToOrd;
        this.ordinalToNames = names;
    }

    /** {@inheritDoc} */
    @Override public String getNameByOrdinal(int ord) throws IgniteCheckedException {
        String name = ordinalToNames.get(ord);
        if (F.isEmpty(name))
            throw new IgniteCheckedException("Unknown ordinal value " + ord + " for enum type '" + typeName + "'");
        return name;
    }

    /** {@inheritDoc} */
    @Override public int getOrdinalByName(String name) throws IgniteCheckedException {
        if (F.isEmpty(name))
            throw new IgniteCheckedException("Empty enum constant name");

        Integer ordinal = nameToOrdinal.get(name.toUpperCase());
        if (ordinal == null)
            throw new IgniteCheckedException("Unknown name value '" + name + "' for enum type '" + typeName + "'");

        return ordinal;
    }

    /** {@inheritDoc} */
    @Override public EnumMetadata merge(EnumMetadata other) {
        assert other instanceof EnumMetadataImpl;

        EnumMetadataImpl otherMeta = (EnumMetadataImpl)other;
        Map<String, Integer> nameToOrd = new LinkedHashMap<>(nameToOrdinal);
        Map<Integer, String> names = new LinkedHashMap<>(((EnumMetadataImpl) other).ordinalToNames);

        for (Map.Entry<String, Integer> e: otherMeta.nameToOrdinal.entrySet()) {
            nameToOrd.put(e.getKey(), e.getValue());
        }

        return new EnumMetadataImpl(this.typeName, nameToOrd, names);
    }

    /**
     * @return Ordinal to name mapping.
     */
    public Map<Integer, String> map() {
        return ordinalToNames;
    }
}
