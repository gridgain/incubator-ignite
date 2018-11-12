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

package org.apache.ignite.internal.sql.ast;

import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * SQL Data type based on H2.
 */
public final class GridSqlType {
    /** */
    public static final int TYPE_UNKNOWN = -1;

    /** */
    public static final int TYPE_BOOLEAN = 1;

    /** */
    public static final int TYPE_LONG = 5;

    /** */
    public static final int TYPE_DOUBLE = 7;

    /** */
    public static final int TYPE_STRING = 13;

    /** */
    public static final int TYPE_RESULT_SET = 18;

    /** */
    public static final int TYPE_UUID = 20;

    /** */
    public static final int TYPE_BOOLEAN_PRECISION = 1;

    /** */
    public static final int TYPE_BOOLEAN_DISPLAY_SIZE = 5;

    /** */
    public static final int TYPE_LONG_PRECISION = 19;

    /** */
    public static final int TYPE_LONG_DISPLAY_SIZE = 20;

    /** */
    public static final int TYPE_DOUBLE_PRECISION = 17;

    /** */
    public static final int TYPE_DOUBLE_DISPLAY_SIZE = 24;

    /** */
    public static final GridSqlType UNKNOWN = new GridSqlType(TYPE_UNKNOWN, 0, 0, 0, null);

    /** */
    public static final GridSqlType BIGINT = new GridSqlType(TYPE_LONG, 0, TYPE_LONG_PRECISION,
        TYPE_LONG_DISPLAY_SIZE, "BIGINT");

    /** */
    public static final GridSqlType DOUBLE = new GridSqlType(TYPE_DOUBLE, 0, TYPE_DOUBLE_PRECISION,
        TYPE_DOUBLE_DISPLAY_SIZE, "DOUBLE");

    /** */
    public static final GridSqlType UUID = new GridSqlType(TYPE_UUID, 0, Integer.MAX_VALUE, 36, "UUID");

    /** */
    public static final GridSqlType BOOLEAN = new GridSqlType(TYPE_BOOLEAN, 0, TYPE_BOOLEAN_PRECISION,
        TYPE_BOOLEAN_DISPLAY_SIZE, "BOOLEAN");

    /** */
    public static final GridSqlType STRING = new GridSqlType(TYPE_STRING, 0, 0, -1, "VARCHAR");

    /** */
    public static final GridSqlType RESULT_SET = new GridSqlType(TYPE_RESULT_SET, 0,
        Integer.MAX_VALUE, Integer.MAX_VALUE, "");

    /** H2 type. */
    private final int type;

    /** */
    private final int scale;

    /** */
    private final long precision;

    /** */
    private final int displaySize;

    /** */
    private final String sql;

    /**
     * @param type H2 Type.
     * @param scale Scale.
     * @param precision Precision.
     * @param displaySize Display size.
     * @param sql SQL definition of the type.
     */
    public GridSqlType(int type, int scale, long precision, int displaySize, String sql) {
        assert !F.isEmpty(sql) || type == TYPE_UNKNOWN || type == TYPE_RESULT_SET;

        this.type = type;
        this.scale = scale;
        this.precision = precision;
        this.displaySize = displaySize;
        this.sql = sql;
    }

    /**
     * @return Get H2 type.
     */
    public int type() {
        return type;
    }

    /**
     * Get the scale of this expression.
     *
     * @return Scale.
     */
    public int scale() {
        return scale;
    }

    /**
     * Get the precision of this expression.
     *
     * @return Precision.
     */
    public long precision() {
        return precision;
    }

    /**
     * Get the display size of this expression.
     *
     * @return the display size
     */
    public int displaySize() {
        return displaySize;
    }

    /**
     * @return SQL definition of the type.
     */
    public String sql() {
        return sql;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSqlType.class, this);
    }
}