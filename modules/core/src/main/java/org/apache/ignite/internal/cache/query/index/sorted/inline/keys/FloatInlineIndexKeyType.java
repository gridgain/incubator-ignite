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

import org.apache.ignite.internal.cache.query.index.sorted.inline.IndexKeyTypes;
import org.apache.ignite.internal.pagemem.PageUtils;

/**
 * Inline index key implementation for inlining {@link Float} values.
 */
public class FloatInlineIndexKeyType extends NullableInlineIndexKeyType<Float> {
    /** */
    public FloatInlineIndexKeyType() {
        super(IndexKeyTypes.FLOAT, (short)4);
    }

    /** {@inheritDoc} */
    @Override public int compare0(long pageAddr, int off, Float v) {
        float val1 = Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1));

        return Integer.signum(Float.compare(val1, v));
    }

    /** {@inheritDoc} */
    @Override protected int put0(long pageAddr, int off, Float val, int maxSize) {
        PageUtils.putByte(pageAddr, off, (byte) type());
        PageUtils.putInt(pageAddr, off + 1, Float.floatToIntBits(val));

        return keySize + 1;
    }

    /** {@inheritDoc} */
    @Override protected Float get0(long pageAddr, int off) {
        return Float.intBitsToFloat(PageUtils.getInt(pageAddr, off + 1));
    }

    /** {@inheritDoc} */
    @Override protected int inlineSize0(Float val) {
        return keySize + 1;
    }
}
