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

package org.apache.ignite.internal.processors.query.h2.opt;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.h2.value.Value;

import java.util.Arrays;
import java.util.Objects;

/**
 * Simple array based row.
 */
public class H2PlainRow extends H2Row {
    /** */
    @GridToStringInclude
    private Value[] vals;

    /**
     * @param vals Values.
     */
    @SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
    public H2PlainRow(Value[] vals) {
        this.vals = vals;
    }

    /**
     * @param len Length.
     */
    public H2PlainRow(int len) {
        vals = new Value[len];
    }

    /** {@inheritDoc} */
    @Override public int getColumnCount() {
        return vals.length;
    }

    /** {@inheritDoc} */
    @Override public Value getValue(int idx) {
        return vals[idx];
    }

    /** {@inheritDoc} */
    @Override public void setValue(int idx, Value v) {
        vals[idx] = v;
    }

    /** {@inheritDoc} */
    @Override public boolean indexSearchRow() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(H2PlainRow.class, this);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return Arrays.hashCode(vals);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj instanceof H2PlainRow) {
            H2PlainRow other = (H2PlainRow)obj;

            return Arrays.equals(vals, other.vals);
        }

        return false;
    }
}
