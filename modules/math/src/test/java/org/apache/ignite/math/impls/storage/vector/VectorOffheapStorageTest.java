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

package org.apache.ignite.math.impls.storage.vector;

import org.apache.ignite.math.impls.MathTestConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link DenseLocalOffHeapVectorStorage}.
 */
public class VectorOffheapStorageTest extends VectorBaseStorageTest<DenseLocalOffHeapVectorStorage> {
    /** */
    @Before
    public void setUp() {
        storage = new DenseLocalOffHeapVectorStorage(MathTestConstants.STORAGE_SIZE);
    }

    /** */
    @Test
    public void isArrayBased() throws Exception {
        assertFalse(MathTestConstants.UNEXPECTED_VAL, storage.isArrayBased());
    }

    /** */
    @Test
    public void data() throws Exception {
        assertNull(MathTestConstants.NULL_VAL, storage.data());
    }

    /** */
    @Test
    public void isSequentialAccess() throws Exception {
        assertTrue(MathTestConstants.UNEXPECTED_VAL, storage.isSequentialAccess());
    }

    /** */
    @Test
    public void isDense() throws Exception {
        assertTrue(MathTestConstants.UNEXPECTED_VAL, storage.isDense());
    }

    /** */
    @Test
    public void equalsTest() {
        //noinspection EqualsWithItself
        assertTrue(MathTestConstants.VAL_NOT_EQUALS, storage.equals(storage));

        //noinspection EqualsBetweenInconvertibleTypes
        assertFalse(MathTestConstants.VALUES_SHOULD_BE_NOT_EQUALS,
            storage.equals(new ArrayVectorStorage(MathTestConstants.STORAGE_SIZE)));
    }

}
