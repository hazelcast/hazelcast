/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.impl.physical.PhysicalNode;
import com.hazelcast.sql.impl.physical.RootPhysicalNode;
import com.hazelcast.sql.impl.physical.io.EdgeAwarePhysicalNode;
import com.hazelcast.sql.impl.row.HeapRow;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Query plan descriptor.
 */
public final class QueryExplain {
    /** Original SQL. */
    private final String sql;

    /** Elements.  */
    private final List<QueryExplainElement> elements;

    private QueryExplain(String sql, List<QueryExplainElement> elements) {
        this.sql = sql;
        this.elements = elements;
    }

    public static QueryExplain fromPlan(String sql, QueryPlan plan) {
        QueryFragment rootFragment = getRootFragment(plan.getFragments());

        PlanCollector collector = new PlanCollector(plan.getFragments(), plan.getOutboundEdgeMap(), rootFragment);

        List<QueryExplainElement> elements = collector.collect();

        return new QueryExplain(sql, elements);
    }

    public SqlCursor asCursor() {
        List<SqlRow> rows = new ArrayList<>(elements.size());

        for (QueryExplainElement element : elements) {
            String elementString = elementAsString(element);

            HeapRow row = new HeapRow(1);
            row.set(0, elementString);

            rows.add(row);
        }

        return new Cursor(rows);
    }

    public String getSql() {
        return sql;
    }

    public List<QueryExplainElement> getElements() {
        return elements;
    }

    private static QueryFragment getRootFragment(List<QueryFragment> fragments) {
        QueryFragment rootFragment = null;

        for (QueryFragment fragment : fragments) {
            if (fragment.getNode() instanceof RootPhysicalNode) {
                rootFragment = fragment;

                break;
            }
        }

        assert rootFragment != null;

        return rootFragment;
    }

    private static String elementAsString(QueryExplainElement element) {
        StringBuilder res = new StringBuilder();

        for (int i = 0; i < element.getLevel(); i++) {
            res.append("  ");
        }

        // TODO: Use special "explain" method here to avoid printing trash.
        res.append(element.getNode().toString());

        return res.toString();
    }

    /**
     * Plan collector.
     */
    private static final class PlanCollector {
        /** Participating fragments. */
        private final List<QueryFragment> fragments;

        /** Map from the edge ID to it's sending fragment index. */
        private final Map<Integer, Integer> outboundEdgeMap;

        /** Collected elements. */
        private final List<QueryExplainElement> elements = new ArrayList<>(1);

        /** Current fragment. */
        private QueryFragment curFragment;

        /** Current level. */
        private int level;

        private PlanCollector(List<QueryFragment> fragments, Map<Integer, Integer> outboundEdgeMap, QueryFragment curFragment) {
            this.fragments = fragments;
            this.outboundEdgeMap = outboundEdgeMap;
            this.curFragment = curFragment;
        }

        private List<QueryExplainElement> collect() {
            PhysicalNode node = curFragment.getNode();

            processNode(node);

            return elements;
        }

        private void processNode(PhysicalNode node) {
            add(node);

            if (node instanceof EdgeAwarePhysicalNode) {
                EdgeAwarePhysicalNode node0 = (EdgeAwarePhysicalNode) node;

                if (!node0.isSender()) {
                    assert node0.getInputCount() == 0;

                    QueryFragment childFragment = fragmentByEdge(node0.getEdgeId());

                    processChildFragment(childFragment);
                }
            }

            for (int i = 0; i < node.getInputCount(); i++) {
                PhysicalNode child = node.getInput(i);

                processChildNode(child);
            }
        }

        private void add(PhysicalNode node) {
            elements.add(new QueryExplainElement(curFragment, node, level));
        }

        private void processChildNode(PhysicalNode node) {
            level++;

            processNode(node);

            level--;
        }

        private void processChildFragment(QueryFragment fragment) {
            QueryFragment oldFragment = curFragment;

            curFragment = fragment;

            processChildNode(fragment.getNode());

            curFragment = oldFragment;
        }

        private QueryFragment fragmentByEdge(int edgeId) {
            int fragmentIndex = outboundEdgeMap.get(edgeId);

            assert fragmentIndex < fragments.size();

            return fragments.get(fragmentIndex);
        }
    }

    /**
     * Cursor to iterate over EXPLAIN rows.
     */
    private static final class Cursor implements SqlCursor {
        /** Rows. */
        private final List<SqlRow> rows;

        private Cursor(List<SqlRow> rows) {
            this.rows = rows;
        }

        @Override
        public Iterator<SqlRow> iterator() {
            return rows.iterator();
        }

        @Override
        public void close() {
            // No-op.
        }
    }
}
