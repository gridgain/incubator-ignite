package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.List;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteCorrelatedNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteMapAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteNestedLoopJoin;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReduceAggregate;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRelVisitor;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSender;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteUnionAll;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/** */
public class IgniteRelShuttle implements IgniteRelVisitor<IgniteRel> {
    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSender rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteFilter rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTrimExchange rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteProject rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteNestedLoopJoin rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteCorrelatedNestedLoopJoin rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteExchange rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteAggregate rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteMapAggregate rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReduceAggregate rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableModify rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteUnionAll rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteSort rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteIndexScan rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteTableScan rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteReceiver rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteValues rel) {
        return processNode(rel);
    }

    /** {@inheritDoc} */
    @Override public IgniteRel visit(IgniteRel rel) {
        return rel.accept(this);
    }

    /**
     * Visits all children of a parent.
     */
    protected IgniteRel processNode(IgniteRel rel) {
        List<IgniteRel> inputs = Commons.cast(rel.getInputs());

        for (int i = 0; i < inputs.size(); i++)
            visitChild(rel, i, inputs.get(i));

        return rel;
    }

    /**
     * Visits a particular child of a parent and replaces the child if it was changed.
     */
    protected void visitChild(IgniteRel parent, int i, IgniteRel child) {
        IgniteRel newChild = visit(child);

        if (newChild != child)
            parent.replaceInput(i, newChild);
    }
}
