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

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.builder.BinaryObjectBuilderImpl;
import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;

/**
 * Thin client implementation of {@link IgniteBinary}.
 */
class ClientBinary implements IgniteBinary {
    /** {@inheritDoc} */
    @Override public int typeId(String typeName) {
        return IgniteBinaryMarshaller.INSTANCE.context().typeId(typeName);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T toBinary(Object obj) {
        if (obj == null)
            return null;

        if (obj instanceof IgniteBinary)
            return (T)obj;

        byte[] objBytes = IgniteBinaryMarshaller.INSTANCE.marshal(obj);

        return (T)IgniteBinaryMarshaller.INSTANCE.unmarshal(new BinaryHeapInputStream(objBytes));
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(String typeName) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("typeName");

        return new BinaryObjectBuilderImpl(IgniteBinaryMarshaller.INSTANCE.context(), typeName);
    }

    /** {@inheritDoc} */
    @Override public BinaryObjectBuilder builder(BinaryObject binaryObj) {
        if (binaryObj == null)
            throw new NullPointerException("binaryObj");

        return BinaryObjectBuilderImpl.wrap(binaryObj);
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(Class<?> cls) throws BinaryObjectException {
        if (cls == null)
            throw new NullPointerException("cls");

        return type(cls.getName());
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(String typeName) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("typeName");

        int typeId = IgniteBinaryMarshaller.INSTANCE.context().typeId(typeName);

        return type(typeId);
    }

    /** {@inheritDoc} */
    @Override public BinaryType type(int typeId) {
        return IgniteBinaryMarshaller.INSTANCE.context().metadata(typeId);
    }

    /** {@inheritDoc} */
    @Override public Collection<BinaryType> types() {
        return IgniteBinaryMarshaller.INSTANCE.context().metadata();
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, int ord) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("typeName");

        BinaryContext ctx = IgniteBinaryMarshaller.INSTANCE.context();

        int typeId = ctx.typeId(typeName);

        typeName = ctx.userTypeName(typeName);

        ctx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, null, null, null, true, null));

        return new BinaryEnumObjectImpl(ctx, typeId, null, ord);
    }

    /** {@inheritDoc} */
    @Override public BinaryObject buildEnum(String typeName, String name) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("typeName");

        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("name");

        BinaryContext ctx = IgniteBinaryMarshaller.INSTANCE.context();

        int typeId = ctx.typeId(typeName);

        BinaryMetadata metadata = ctx.metadata0(typeId);

        if (metadata == null)
            throw new BinaryObjectException(
                String.format("Failed to get metadata for type [typeId=%s, typeName='%s']", typeId, typeName)
            );

        Integer ordinal = metadata.getEnumOrdinalByName(name);

        if (ordinal == null)
            throw new BinaryObjectException(String.format(
                "Failed to resolve enum ordinal by name [typeId=%s, typeName='%s', name='%s']",
                typeId,
                typeName,
                name
            ));

        return new BinaryEnumObjectImpl(ctx, typeId, null, ordinal);
    }

    /** {@inheritDoc} */
    @Override public BinaryType registerEnum(String typeName, Map<String, Integer> vals) {
        if (typeName == null || typeName.length() == 0)
            throw new IllegalArgumentException("typeName");

        BinaryContext ctx = IgniteBinaryMarshaller.INSTANCE.context();

        int typeId = ctx.typeId(typeName);

        ctx.updateMetadata(typeId, new BinaryMetadata(typeId, typeName, null, null, null, true, vals));

        return ctx.metadata(typeId);
    }
}
