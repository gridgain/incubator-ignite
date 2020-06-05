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
package org.apache.ignite.internal.processors.query.calcite;

import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Limit / offset tests.
 */
public class LimitOffsetTest extends GridCommonAbstractTest {
    /** */
    private static final int ROWS = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        Ignite grid = startGrids(2);

        QueryEntity e = new QueryEntity()
            .setTableName("Test")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .setKeyFieldName("id")
            .setValueFieldName("val")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", String.class.getName(), null);

        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(e);

        IgniteCache<Integer, String> cache = grid.createCache(ccfg);

        for (int i = 0; i < ROWS; ++i)
            cache.put(i, "val_" + i);
    }

    /** */
    private CacheConfiguration cacheConfiguration(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.PARTITIONED)
            .setBackups(1)
            .setQueryEntities(singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    /**
     *
     */
    @Test
    public void testLimitOffset() {
        int[] limits = {-1, 0, 10, ROWS / 2 - 1, ROWS / 2, ROWS / 2 + 1, ROWS - 1, ROWS};
        int[] offsets = {-1, 0, 10, ROWS / 2 - 1, ROWS / 2, ROWS / 2 + 1, ROWS - 1, ROWS};

        for (int lim : limits) {
            for (int off : offsets) {
                log.info("+++ Check [limit=" + lim + ", off=" + off + ']');

                checkQuery(lim, off, false, false);
                checkQuery(lim, off, true, false);
                checkQuery(lim, off, false, true);
                checkQuery(lim, off, true, true);
            }
        }
    }

    /**
     * Check query with specified limit and offset (or without its when the arguments are negative),
     *
     * @param lim Limit.
     * @param off Offset.
     * @param param If {@code false} place limit/offset as literals. Otherwise they are plase as parameters.
     * @param sorted Use sorted query (adds ORDER BY).
     */
    void checkQuery(int lim, int off, boolean param, boolean sorted) {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        String sql = createSql(lim, off, false, false);

        log.info("SQL: " + sql);

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC", sql, param ? new Object[]{off, lim} : X.EMPTY_OBJECT_ARRAY);

        List<List<?>> res = cursors.get(0).getAll();

        assertEquals("Invalid results size. [limit=" + lim + ", off=" + off + ", res=" + res.size() + ']',
            expectedSize(lim, off), res.size());
    }

    /**
     * Calculates expected result set size by limit and offset.
     */
    private int expectedSize(int lim, int off) {
        if (off < 0)
            off = 0;

        if (lim == 0)
            return 0;
        else if (lim < 0)
            return ROWS - off;
        else if (lim + off < ROWS)
            return  lim;
        else if (off > ROWS)
            return  0;
        else
            return  ROWS - off;
    }

    /**
     * @param lim Limit.
     * @param off Offset.
     * @param param Flag to place limit/offset  by parameter or literal.
     * @return SQL query string.
     */
    private String createSql(int lim, int off, boolean param, boolean sorted) {
        StringBuilder sb = new StringBuilder("SELECT * FROM TEST ");

        if (sorted)
            sb.append("ORDER BY ID ");

        if (off >= 0)
            sb.append("OFFSET ").append(param ? "?" : Integer.toString(off)).append(" ROWS ");

        if (lim >= 0)
            sb.append("FETCH FIRST ").append(param ? "?" : Integer.toString(lim)).append(" ROWS ONLY");

        return sb.toString();
    }
}
