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

package org.apache.ignite.internal.cache.query.index.sorted.inline.keys;

import java.util.UUID;
import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link UUID} values.
 */
public class UuidInlineIndexKeyType extends NullableInlineIndexKeyType<UUID> {
    /**
     */
    public UuidInlineIndexKeyType() {
        super(IndexKeyTypes.UUID, (short)16);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, UUID v) {
        long part1 = PageUtils.getLong(pageAddr, off + 1);

        int c = Integer.signum(Long.compare(part1, v.getMostSignificantBits()));

        if (c != 0)
            return c;

        long part2 = PageUtils.getLong(pageAddr, off + 9);

        return Integer.signum(Long.compare(part2, v.getLeastSignificantBits()));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, UUID val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putLong(pageAddr, off + 1, val.getMostSignificantBits());
        PageUtils.putLong(pageAddr, off + 9, val.getLeastSignificantBits());

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected UUID get0(long pageAddr, int off) {
        return new UUID(
            PageUtils.getLong(pageAddr, off + 1),
            PageUtils.getLong(pageAddr, off + 9)
        );
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(UUID val) {
        return keySize + 1;
    }
}
