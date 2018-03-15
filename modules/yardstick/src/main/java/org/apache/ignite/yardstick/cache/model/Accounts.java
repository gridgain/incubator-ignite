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

package org.apache.ignite.yardstick.cache.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

/**
 * Entity class for benchmark.
 */
public class Accounts {
    /** */
    @QuerySqlField
    private long val;


    /** */
    @QuerySqlField
    private long val1;


    /** */
    @QuerySqlField
    private long val2;

    /** */
    @QuerySqlField
    private String s1;

    /** */
    @QuerySqlField
    private String s2;


    /** */
    public Accounts() {
        // No-op.
    }

    /**
     * @param val Id.
     */
    public Accounts(long val) {
        this.val = val;

        this.val1 = val;

        this.val2 = val;

        this.s1 = Long.toBinaryString(val).substring(0, 16);

        this.s2 = Long.toBinaryString(val).substring(0, 16).replace("0", "a");
    }

    /**
     * @param val Val.
     */
    public Accounts setVal(long val) {
        this.val = val;

        return this;
    }

    /**
     * @return Val.
     */
    public long getVal() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "Accounts [val=" + val + ']';
    }
}