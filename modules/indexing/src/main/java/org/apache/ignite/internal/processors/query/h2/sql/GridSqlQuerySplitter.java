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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.query.GridCacheSqlQuery;
import org.apache.ignite.internal.processors.cache.query.GridCacheTwoStepQuery;
import org.apache.ignite.lang.IgnitePredicate;
import org.h2.command.Prepared;
import org.h2.jdbc.JdbcPreparedStatement;
import org.h2.util.IntArray;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.h2.opt.GridH2CollocationModel.isCollocated;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.AVG;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.CAST;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.COUNT;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlFunctionType.SUM;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlPlaceholder.EMPTY;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.prepared;
import static org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser.query;
import static org.apache.ignite.internal.processors.query.h2.twostep.GridReduceQueryExecutor.toArray;

/**
 * Splits a single SQL query into two step map-reduce query.
 */
public class GridSqlQuerySplitter {
    /** */
    private static final String TABLE_SCHEMA = "PUBLIC";

    /** */
    private static final String MERGE_TABLE_PREFIX = "__T";

    /** */
    private static final String COLUMN_PREFIX = "__C";

    /** */
    private static final String HAVING_COLUMN = "__H";

    /** */
    private int nextMergeTblIdx;

    /** */
    private GridCacheSqlQuery mapSqlQry;

    /** */
    private GridCacheSqlQuery rdcSqlQry;

    /** */
    private boolean rdcQrySimple;

    /**
     * @param idx Index of table.
     * @return Merge table.
     */
    private static GridSqlTable mergeTable(int idx) {
        return new GridSqlTable(TABLE_SCHEMA, MERGE_TABLE_PREFIX + idx);
    }

    /**
     * @param idx Table index.
     * @return Merge table name.
     */
    public static String mergeTableIdentifier(int idx) {
        return mergeTable(idx).getSQL();
    }

    /**
     * @param idx Index of column.
     * @return Generated by index column alias.
     */
    private static String columnName(int idx) {
        return COLUMN_PREFIX + idx;
    }

    /**
     * @param qry Query.
     * @return Leftest simple query if this is UNION.
     */
    private static GridSqlSelect leftest(GridSqlQuery qry) {
        if (qry instanceof GridSqlUnion)
            return leftest(((GridSqlUnion)qry).left());

        return (GridSqlSelect)qry;
    }

    /**
     * @param qry Query.
     * @return Select query.
     */
    private static GridSqlSelect wrapUnion(GridSqlQuery qry) {
        if (qry instanceof GridSqlSelect)
            return (GridSqlSelect)qry;

        // Handle UNION.
        GridSqlSelect wrapQry = new GridSqlSelect().from(new GridSqlSubquery(qry));

        wrapQry.explain(qry.explain());
        qry.explain(false);

        GridSqlSelect left = leftest(qry);

        int c = 0;

        for (GridSqlElement expr : left.columns(true)) {
            GridSqlType type = expr.resultType();

            String colName;

            if (expr instanceof GridSqlAlias)
                colName = ((GridSqlAlias)expr).alias();
            else if (expr instanceof GridSqlColumn)
                colName = ((GridSqlColumn)expr).columnName();
            else {
                colName = columnName(c);

                expr = alias(colName, expr);

                // Set generated alias to the expression.
                left.setColumn(c, expr);
            }

            GridSqlColumn col = column(colName);

            col.resultType(type);

            wrapQry.addColumn(col, true);

            c++;
        }

        // ORDER BY
        if (!qry.sort().isEmpty()) {
            for (GridSqlSortColumn col : qry.sort())
                wrapQry.addSort(col);
        }

        return wrapQry;
    }

    /**
     * @param stmt Prepared statement.
     * @param params Parameters.
     * @param collocatedGrpBy Whether the query has collocated GROUP BY keys.
     * @param distributedJoins If distributed joins enabled.
     * @return Two step query.
     */
    public static GridCacheTwoStepQuery split(
        JdbcPreparedStatement stmt,
        Object[] params,
        final boolean collocatedGrpBy,
        final boolean distributedJoins
    ) {
        if (params == null)
            params = GridCacheSqlQuery.EMPTY_PARAMS;

        Set<String> tbls = new HashSet<>();
        Set<String> schemas = new HashSet<>();

        final Prepared prepared = prepared(stmt);

        GridSqlQuery qry = new GridSqlQueryParser().parse(prepared);

        final boolean explain = qry.explain();

        qry.explain(false);

        collectAllTables(qry, schemas, tbls);

        GridSqlQuerySplitter splitter = new GridSqlQuerySplitter();

        // Map query will be direct reference to the original query AST.
        // Thus all the modifications will be performed on the original AST, so we should be careful when
        // nullifying or updating things, have to make sure that we will not need them in the original form later.
        splitter.splitSelect(wrapUnion(qry), params, collocatedGrpBy);

        // Build resulting two step query.
        GridCacheTwoStepQuery twoStepQry = new GridCacheTwoStepQuery(schemas, tbls);

        twoStepQry.addMapQuery(splitter.mapSqlQry);
        twoStepQry.reduceQuery(splitter.rdcSqlQry);
        twoStepQry.skipMergeTable(splitter.rdcQrySimple);
        twoStepQry.explain(explain);

        // We do not have to look at each map query separately here, because if
        // the whole initial query is collocated, then all the map sub-queries
        // will be collocated as well.
        twoStepQry.distributedJoins(distributedJoins && !isCollocated(query(prepared)));

        return twoStepQry;
    }

    /**
     * !!! Notice that here we will modify the original query AST.
     *
     * @param mapQry The original query AST to be split. It will be used as a base for map query.
     * @param params Query parameters.
     * @param collocatedGrpBy Whether the query has collocated GROUP BY keys.
     */
    private void splitSelect(
        final GridSqlSelect mapQry,
        Object[] params,
        boolean collocatedGrpBy
    ) {
        final int visibleCols = mapQry.visibleColumns();

        List<GridSqlElement> rdcExps = new ArrayList<>(visibleCols);
        List<GridSqlElement> mapExps = new ArrayList<>(mapQry.allColumns());

        mapExps.addAll(mapQry.columns(false));

        Set<String> colNames = new HashSet<>();
        final int havingCol = mapQry.havingColumn();

        boolean aggregateFound = false;

        // Split all select expressions into map-reduce parts.
        for (int i = 0, len = mapExps.size(); i < len; i++) // Remember len because mapExps list can grow.
            aggregateFound |= splitSelectExpression(mapExps, rdcExps, colNames, i, collocatedGrpBy, i == havingCol);

        assert !(collocatedGrpBy && aggregateFound); // We do not split aggregates when collocatedGrpBy is true.

        // Create reduce query AST.
        GridSqlSelect rdcQry = new GridSqlSelect().from(mergeTable(nextMergeTblIdx++));

        // -- SELECT
        mapQry.clearColumns();

        for (GridSqlElement exp : mapExps) // Add all map expressions as visible.
            mapQry.addColumn(exp, true);

        for (int i = 0; i < visibleCols; i++) // Add visible reduce columns.
            rdcQry.addColumn(rdcExps.get(i), true);

        for (int i = visibleCols; i < rdcExps.size(); i++) // Add invisible reduce columns (HAVING).
            rdcQry.addColumn(rdcExps.get(i), false);

        for (int i = rdcExps.size(); i < mapExps.size(); i++)  // Add all extra map columns as invisible reduce columns.
            rdcQry.addColumn(column(((GridSqlAlias)mapExps.get(i)).alias()), false);

        // -- FROM TODO
        mapQry.from();

        // -- WHERE TODO
        mapQry.where();

        // -- GROUP BY
        if (mapQry.groupColumns() != null && !collocatedGrpBy)
            rdcQry.groupColumns(mapQry.groupColumns());

        // -- HAVING
        if (havingCol >= 0 && !collocatedGrpBy) {
            // TODO IGNITE-1140 - Find aggregate functions in HAVING clause or rewrite query to put all aggregates to SELECT clause.
            // We need to find HAVING column in reduce query.
            for (int i = visibleCols; i < rdcQry.allColumns(); i++) {
                GridSqlElement c = rdcQry.column(i);

                if (c instanceof GridSqlAlias && HAVING_COLUMN.equals(((GridSqlAlias)c).alias())) {
                    rdcQry.havingColumn(i);

                    break;
                }
            }

            mapQry.havingColumn(-1);
        }

        // -- ORDER BY
        if (!mapQry.sort().isEmpty()) {
            for (GridSqlSortColumn sortCol : mapQry.sort())
                rdcQry.addSort(sortCol);

            // If collocatedGrpBy is true, then aggregateFound is always false.
            if (aggregateFound) // Ordering over aggregates does not make sense.
                mapQry.clearSort(); // Otherwise map sort will be used by offset-limit.
            // TODO IGNITE-1141 - Check if sorting is done over aggregated expression, otherwise we can sort and use offset-limit.
        }

        // -- LIMIT
        if (mapQry.limit() != null) {
            rdcQry.limit(mapQry.limit());

            // Will keep limits on map side when collocatedGrpBy is true,
            // because in this case aggregateFound is always false.
            if (aggregateFound)
                mapQry.limit(null);
        }

        // -- OFFSET
        if (mapQry.offset() != null) {
            rdcQry.offset(mapQry.offset());

            if (mapQry.limit() != null) // LIMIT off + lim
                mapQry.limit(op(GridSqlOperationType.PLUS, mapQry.offset(), mapQry.limit()));

            mapQry.offset(null);
        }

        // -- DISTINCT
        if (mapQry.distinct()) {
            mapQry.distinct(!aggregateFound && mapQry.groupColumns() == null && mapQry.havingColumn() < 0);
            rdcQry.distinct(true);
        }

        IntArray paramIdxs = new IntArray(params.length);

        GridCacheSqlQuery map = new GridCacheSqlQuery(mapQry.getSQL(),
            findParams(mapQry, params, new ArrayList<>(params.length), paramIdxs).toArray());

        map.columns(collectColumns(mapExps));
        map.parameterIndexes(toArray(paramIdxs));

        paramIdxs = new IntArray(params.length);

        GridCacheSqlQuery rdc = new GridCacheSqlQuery(rdcQry.getSQL(),
            findParams(rdcQry, params, new ArrayList<>(), paramIdxs).toArray());

        rdc.parameterIndexes(toArray(paramIdxs));

        // Setup result fields.
        mapSqlQry = map;
        rdcSqlQry = rdc;
        rdcQrySimple = rdcQry.simpleQuery();
    }

    /**
     * @param cols Columns from SELECT clause.
     * @return Map of columns with types.
     */
    private static LinkedHashMap<String,?> collectColumns(List<GridSqlElement> cols) {
        LinkedHashMap<String, GridSqlType> res = new LinkedHashMap<>(cols.size(), 1f, false);

        for (int i = 0; i < cols.size(); i++) {
            GridSqlElement col = cols.get(i);
            GridSqlType t = col.resultType();

            if (t == null)
                throw new NullPointerException("Column type.");

            if (t == GridSqlType.UNKNOWN)
                throw new IllegalStateException("Unknown type: " + col);

            String alias;

            if (col instanceof GridSqlAlias)
                alias = ((GridSqlAlias)col).alias();
            else
                alias = columnName(i);

            if (res.put(alias, t) != null)
                throw new IllegalStateException("Alias already exists: " + alias);
        }

        return res;
    }

    /**
     * @param qry Query.
     * @param schemas Schema names.
     * @param tbls Tables.
     */
    private static void collectAllTables(GridSqlQuery qry, Set<String> schemas, Set<String> tbls) {
        if (qry instanceof GridSqlUnion) {
            GridSqlUnion union = (GridSqlUnion)qry;

            collectAllTables(union.left(), schemas, tbls);
            collectAllTables(union.right(), schemas, tbls);
        }
        else {
            GridSqlSelect select = (GridSqlSelect)qry;

            collectAllTablesInFrom(select.from(), schemas, tbls);

            for (GridSqlElement el : select.columns(false))
                collectAllTablesInSubqueries(el, schemas, tbls);

            collectAllTablesInSubqueries(select.where(), schemas, tbls);
        }
    }

    /**
     * @param from From element.
     * @param schemas Schema names.
     * @param tbls Tables.
     */
    private static void collectAllTablesInFrom(GridSqlElement from, final Set<String> schemas, final Set<String> tbls) {
        findTablesInFrom(from, new IgnitePredicate<GridSqlElement>() {
            @Override public boolean apply(GridSqlElement el) {
                if (el instanceof GridSqlTable) {
                    GridSqlTable tbl = (GridSqlTable)el;

                    String schema = tbl.schema();

                    boolean addSchema = tbls == null;

                    if (tbls != null)
                        addSchema = tbls.add(tbl.dataTable().identifier());

                    if (addSchema && schema != null && schemas != null)
                        schemas.add(schema);
                }
                else if (el instanceof GridSqlSubquery)
                    collectAllTables(((GridSqlSubquery)el).subquery(), schemas, tbls);

                return false;
            }
        });
    }

    /**
     * Processes all the tables and subqueries using the given closure.
     *
     * @param from FROM element.
     * @param c Closure each found table and subquery will be passed to. If returns {@code true} the we need to stop.
     * @return {@code true} If we have found.
     */
    private static boolean findTablesInFrom(GridSqlElement from, IgnitePredicate<GridSqlElement> c) {
        if (from == null)
            return false;

        if (from instanceof GridSqlTable || from instanceof GridSqlSubquery)
            return c.apply(from);

        if (from instanceof GridSqlJoin) {
            // Left and right.
            if (findTablesInFrom(from.child(0), c))
                return true;

            if (findTablesInFrom(from.child(1), c))
                return true;

            // We don't process ON condition because it is not a joining part of from here.
            return false;
        }
        else if (from instanceof GridSqlAlias)
            return findTablesInFrom(from.child(), c);
        else if (from instanceof GridSqlFunction)
            return false;

        throw new IllegalStateException(from.getClass().getName() + " : " + from.getSQL());
    }

    /**
     * Searches schema names and tables in subqueries in SELECT and WHERE clauses.
     *
     * @param el Element.
     * @param schemas Schema names.
     * @param tbls Tables.
     */
    private static void collectAllTablesInSubqueries(GridSqlElement el, Set<String> schemas, Set<String> tbls) {
        if (el == null)
            return;

        el = GridSqlAlias.unwrap(el);

        if (el instanceof GridSqlOperation || el instanceof GridSqlFunction) {
            for (GridSqlElement child : el)
                collectAllTablesInSubqueries(child, schemas, tbls);
        }
        else if (el instanceof GridSqlSubquery)
            collectAllTables(((GridSqlSubquery)el).subquery(), schemas, tbls);
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlQuery qry, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (qry instanceof GridSqlSelect)
            return findParams((GridSqlSelect)qry, params, target, paramIdxs);

        GridSqlUnion union = (GridSqlUnion)qry;

        findParams(union.left(), params, target, paramIdxs);
        findParams(union.right(), params, target, paramIdxs);

        findParams(qry.limit(), params, target, paramIdxs);
        findParams(qry.offset(), params, target, paramIdxs);

        return target;
    }

    /**
     * @param qry Select.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     * @return Extracted parameters list.
     */
    private static List<Object> findParams(GridSqlSelect qry, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (params.length == 0)
            return target;

        for (GridSqlElement el : qry.columns(false))
            findParams(el, params, target, paramIdxs);

        findParams(qry.from(), params, target, paramIdxs);
        findParams(qry.where(), params, target, paramIdxs);

        // Don't search in GROUP BY and HAVING since they expected to be in select list.

        findParams(qry.limit(), params, target, paramIdxs);
        findParams(qry.offset(), params, target, paramIdxs);

        return target;
    }

    /**
     * @param el Element.
     * @param params Parameters.
     * @param target Extracted parameters.
     * @param paramIdxs Parameter indexes.
     */
    private static void findParams(@Nullable GridSqlElement el, Object[] params, ArrayList<Object> target,
        IntArray paramIdxs) {
        if (el == null)
            return;

        if (el instanceof GridSqlParameter) {
            // H2 Supports queries like "select ?5" but first 4 non-existing parameters are need to be set to any value.
            // Here we will set them to NULL.
            final int idx = ((GridSqlParameter)el).index();

            while (target.size() < idx)
                target.add(null);

            if (params.length <= idx)
                throw new IgniteException("Invalid number of query parameters. " +
                    "Cannot find " + idx + " parameter.");

            Object param = params[idx];

            if (idx == target.size())
                target.add(param);
            else
                target.set(idx, param);

            paramIdxs.add(idx);
        }
        else if (el instanceof GridSqlSubquery)
            findParams(((GridSqlSubquery)el).subquery(), params, target, paramIdxs);
        else
            for (GridSqlElement child : el)
                findParams(child, params, target, paramIdxs);
    }

    /**
     * @param mapSelect Selects for map query.
     * @param rdcSelect Selects for reduce query.
     * @param colNames Set of unique top level column names.
     * @param idx Index.
     * @param collocatedGrpBy If it is a collocated GROUP BY query.
     * @param isHaving If it is a HAVING expression.
     * @return {@code true} If aggregate was found.
     */
    private static boolean splitSelectExpression(
        List<GridSqlElement> mapSelect,
        List<GridSqlElement> rdcSelect,
        Set<String> colNames,
        final int idx,
        boolean collocatedGrpBy,
        boolean isHaving
    ) {
        GridSqlElement el = mapSelect.get(idx);
        GridSqlAlias alias = null;

        boolean aggregateFound = false;

        if (el instanceof GridSqlAlias) { // Unwrap from alias.
            alias = (GridSqlAlias)el;
            el = alias.child();
        }

        if (!collocatedGrpBy && hasAggregates(el)) {
            aggregateFound = true;

            if (alias == null)
                alias = alias(isHaving ? HAVING_COLUMN : columnName(idx), el);

            // We can update original alias here as well since it will be dropped from mapSelect.
            splitAggregates(alias, 0, mapSelect, idx, true);

            set(rdcSelect, idx, alias);
        }
        else {
            String mapColAlias = isHaving ? HAVING_COLUMN : columnName(idx);
            String rdcColAlias;

            if (alias == null)  // Original column name for reduce column.
                rdcColAlias = el instanceof GridSqlColumn ? ((GridSqlColumn)el).columnName() : mapColAlias;
            else // Set initial alias for reduce column.
                rdcColAlias = alias.alias();

            // Always wrap map column into generated alias.
            mapSelect.set(idx, alias(mapColAlias, el)); // `el` is known not to be an alias.

            // SELECT __C0 AS original_alias
            GridSqlElement rdcEl = column(mapColAlias);

            if (colNames.add(rdcColAlias)) // To handle column name duplication (usually wildcard for few tables).
                rdcEl = alias(rdcColAlias, rdcEl);

            set(rdcSelect, idx, rdcEl);
        }

        return aggregateFound;
    }

    /**
     * @param list List.
     * @param idx Index.
     * @param item Element.
     */
    private static <Z> void set(List<Z> list, int idx, Z item) {
        assert list.size() == idx;
        list.add(item);
    }

    /**
     * @param el Expression.
     * @return {@code true} If expression contains aggregates.
     */
    private static boolean hasAggregates(GridSqlElement el) {
        if (el instanceof GridSqlAggregateFunction)
            return true;

        for (GridSqlElement child : el) {
            if (hasAggregates(child))
                return true;
        }

        return false;
    }

    /**
     * @param parentExpr Parent expression.
     * @param childIdx Child index to try to split.
     * @param mapSelect List of expressions in map SELECT clause.
     * @param exprIdx Index of the original expression in map SELECT clause.
     * @param first If the first aggregate is already found in this expression.
     * @return {@code true} If the first aggregate is already found.
     */
    private static boolean splitAggregates(
        final GridSqlElement parentExpr,
        final int childIdx,
        final List<GridSqlElement> mapSelect,
        final int exprIdx,
        boolean first) {
        GridSqlElement el = parentExpr.child(childIdx);

        if (el instanceof GridSqlAggregateFunction) {
            splitAggregate(parentExpr, childIdx, mapSelect, exprIdx, first);

            return true;
        }

        for (int i = 0; i < el.size(); i++) {
            if (splitAggregates(el, i, mapSelect, exprIdx, first))
                first = false;
        }

        return !first;
    }

    /**
     * @param parentExpr Parent expression.
     * @param aggIdx Index of the aggregate to split in this expression.
     * @param mapSelect List of expressions in map SELECT clause.
     * @param exprIdx Index of the original expression in map SELECT clause.
     * @param first If this is the first aggregate found in this expression.
     */
    private static void splitAggregate(
        GridSqlElement parentExpr,
        int aggIdx,
        List<GridSqlElement> mapSelect,
        int exprIdx,
        boolean first
    ) {
        GridSqlAggregateFunction agg = parentExpr.child(aggIdx);

        assert agg.resultType() != null;

        GridSqlElement mapAgg, rdcAgg;

        // Create stubbed map alias to fill it with correct expression later.
        GridSqlAlias mapAggAlias = alias(columnName(first ? exprIdx : mapSelect.size()), EMPTY);

        // Replace original expression if it is the first aggregate in expression or add to the end.
        if (first)
            mapSelect.set(exprIdx, mapAggAlias);
        else
            mapSelect.add(mapAggAlias);

        switch (agg.type()) {
            case AVG: // SUM( AVG(CAST(x AS DOUBLE))*COUNT(x) )/SUM( COUNT(x) ).
                //-- COUNT(x) map
                GridSqlElement cntMapAgg = aggregate(agg.distinct(), COUNT)
                    .resultType(GridSqlType.BIGINT).addChild(agg.child());

                // Add generated alias to COUNT(x).
                // Using size as index since COUNT will be added as the last select element to the map query.
                String cntMapAggAlias = columnName(mapSelect.size());

                cntMapAgg = alias(cntMapAggAlias, cntMapAgg);

                mapSelect.add(cntMapAgg);

                //-- AVG(CAST(x AS DOUBLE)) map
                mapAgg = aggregate(agg.distinct(), AVG).resultType(GridSqlType.DOUBLE).addChild(
                    function(CAST).resultType(GridSqlType.DOUBLE).addChild(agg.child()));

                //-- SUM( AVG(x)*COUNT(x) )/SUM( COUNT(x) ) reduce
                GridSqlElement sumUpRdc = aggregate(false, SUM).addChild(
                    op(GridSqlOperationType.MULTIPLY,
                        column(mapAggAlias.alias()),
                        column(cntMapAggAlias)));

                GridSqlElement sumDownRdc = aggregate(false, SUM).addChild(column(cntMapAggAlias));

                rdcAgg = op(GridSqlOperationType.DIVIDE, sumUpRdc, sumDownRdc);

                break;

            case SUM: // SUM( SUM(x) )
            case MAX: // MAX( MAX(x) )
            case MIN: // MIN( MIN(x) )
                mapAgg = aggregate(agg.distinct(), agg.type()).resultType(agg.resultType()).addChild(agg.child());
                rdcAgg = aggregate(agg.distinct(), agg.type()).addChild(column(mapAggAlias.alias()));

                break;

            case COUNT_ALL: // CAST(SUM( COUNT(*) ) AS BIGINT)
            case COUNT: // CAST(SUM( COUNT(x) ) AS BIGINT)
                mapAgg = aggregate(agg.distinct(), agg.type()).resultType(GridSqlType.BIGINT);

                if (agg.type() == COUNT)
                    mapAgg.addChild(agg.child());

                rdcAgg = aggregate(false, SUM).addChild(column(mapAggAlias.alias()));
                rdcAgg = function(CAST).resultType(GridSqlType.BIGINT).addChild(rdcAgg);

                break;

            default:
                throw new IgniteException("Unsupported aggregate: " + agg.type());
        }

        assert !(mapAgg instanceof GridSqlAlias);
        assert mapAgg.resultType() != null;

        // Fill the map alias with aggregate.
        mapAggAlias.child(0, mapAgg);
        mapAggAlias.resultType(mapAgg.resultType());

        // Replace in original expression aggregate with reduce aggregate.
        parentExpr.child(aggIdx, rdcAgg);
    }

    /**
     * @param distinct Distinct.
     * @param type Type.
     * @return Aggregate function.
     */
    private static GridSqlAggregateFunction aggregate(boolean distinct, GridSqlFunctionType type) {
        return new GridSqlAggregateFunction(distinct, type);
    }

    /**
     * @param name Column name.
     * @return Column.
     */
    private static GridSqlColumn column(String name) {
        return new GridSqlColumn(null, null, name, name);
    }

    /**
     * @param alias Alias.
     * @param child Child.
     * @return Alias.
     */
    private static GridSqlAlias alias(String alias, GridSqlElement child) {
        GridSqlAlias res = new GridSqlAlias(alias, child);

        res.resultType(child.resultType());

        return res;
    }

    /**
     * @param type Type.
     * @param left Left expression.
     * @param right Right expression.
     * @return Binary operator.
     */
    private static GridSqlOperation op(GridSqlOperationType type, GridSqlElement left, GridSqlElement right) {
        return new GridSqlOperation(type, left, right);
    }

    /**
     * @param type Type.
     * @return Function.
     */
    private static GridSqlFunction function(GridSqlFunctionType type) {
        return new GridSqlFunction(type);
    }
}