/*
 * Copyright 2020 GridGain Systems, Inc. and Contributors.
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

package org.apache.ignite.internal.processors.query.calcite.sql;

import org.apache.calcite.sql.IgniteSqlNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

/** An AST node representing option to create table with. */
public class IgniteSqlCreateTableOption extends IgniteSqlNode {
    /** Option key. */
    private final IgniteSqlCreateTableOptionEnum key;

    /** Option value. */
    private final SqlNode value;

    /** Creates IgniteSqlCreateTableOption. */
    public IgniteSqlCreateTableOption(IgniteSqlCreateTableOptionEnum key, SqlNode value, SqlParserPos pos) {
        super(pos);

        this.key = key;
        this.value = value;
    }

    /** {@inheritDoc} */
    @Override public SqlNode clone(SqlParserPos pos) {
        return new IgniteSqlCreateTableOption(key, value, pos);
    }

    /** {@inheritDoc} */
    @Override public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(key.name());
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }

    /** {@inheritDoc} */
    @Override public void validate(SqlValidator validator, SqlValidatorScope scope) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public <R> R accept(SqlVisitor<R> visitor) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof IgniteSqlCreateTableOption))
            return litmus.fail("{} != {}", this, node);

        IgniteSqlCreateTableOption that = (IgniteSqlCreateTableOption)node;
        if (key != that.key)
            return litmus.fail("{} != {}", this, node);

        return value.equalsDeep(that.value, litmus);
    }

    /**
     * @return Option's key.
     */
    public IgniteSqlCreateTableOptionEnum key() {
        return key;
    }

    /**
     * @return Option's value.
     */
    public SqlNode value() {
        return value;
    }
}
