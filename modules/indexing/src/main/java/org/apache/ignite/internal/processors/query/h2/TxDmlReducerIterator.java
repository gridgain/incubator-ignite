/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.h2;

import java.util.Iterator;
import java.util.List;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.query.h2.dml.UpdatePlan;
import org.apache.ignite.internal.util.lang.GridIteratorAdapter;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Iterates over results of TX DML reducer.
 */
class TxDmlReducerIterator extends GridIteratorAdapter<IgniteBiTuple> {
    /** */
    private final UpdatePlan plan;

    /** */
    private final Iterator<List<?>> it;

    /**
     *
     * @param plan Update plan.
     * @param cur Cursor.
     */
    TxDmlReducerIterator(UpdatePlan plan, Iterable<List<?>> cur) {
        this.plan = plan;

        it = cur.iterator();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNextX() throws IgniteCheckedException {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Override public IgniteBiTuple nextX() throws IgniteCheckedException {
        switch (plan.mode()) {
            case INSERT:
            case MERGE:
                return plan.processRow(it.next());

            case UPDATE: {
                T3<Object, Object, Object> row = plan.processRowForUpdate(it.next());

                return new IgniteBiTuple<>(row.get1(), row.get3());
            }
            case DELETE: {
                List<?> row = it.next();

                return new IgniteBiTuple<>(row.get(0), row.get(1));
            }

            default:
                throw new UnsupportedOperationException(String.valueOf(plan.mode()));
        }
    }

    /** {@inheritDoc} */
    @Override public void removeX() throws IgniteCheckedException {
        // No-op.
    }
}
