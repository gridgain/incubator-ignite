/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.randomsql.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.compatibility.sql.randomsql.Column;
import org.apache.ignite.compatibility.sql.randomsql.Table;

/**
 *
 */
public class TableRef extends Ast {
    /** */
    private static final AtomicLong ALIAS_ID = new AtomicLong();

    /** */
    private final Table tbl;

    /** */
    private final String alias;

    /** */
    private final List<ColumnRef> colRefs = new ArrayList<>();

    /** */
    public TableRef(Ast parent, Table tbl) {
        super(parent);
        this.tbl = tbl;
        alias = tbl.name().substring(0, 1) + ALIAS_ID.incrementAndGet();
        for (Column col : tbl.columnsList())
            colRefs.add(new ColumnRef(parent, col, alias));
    }

    /** {@inheritDoc} */
    @Override public void print(StringBuilder out) {
        out.append(" ")
            .append(tbl.name()).append(" ")
            .append(alias).append(" ");
    }

    /** */
    public List<ColumnRef> columnRefs() {
        return colRefs;
    }
}
