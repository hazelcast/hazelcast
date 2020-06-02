/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.calcite.opt;

import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.explain.QueryExplainElement;
import com.hazelcast.sql.impl.calcite.opt.cost.Cost;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import org.apache.calcite.plan.HazelcastRelOptCluster;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Helper class to create EXPLAIN.
 */
public final class ExplainCreator {
    /** Name of the field with collected values in RelWriterImpl. */
    private static final String REL_WRITER_VALUES_FIELD_NAME = "values";

    /** Field with collected values in RelWriterImpl. */
    private static final Field REL_WRITER_VALUES_FIELD;

    /** Root node. */
    private final PhysicalRel root;

    /** ID map. */
    private final Map<PhysicalRel, List<Integer>> idMap;

    /** Explain elements. */
    private final List<QueryExplainElement> elements = new ArrayList<>();

    /** Current level. */
    private int level;

    static {
        try {
            REL_WRITER_VALUES_FIELD = RelWriterImpl.class.getDeclaredField(REL_WRITER_VALUES_FIELD_NAME);
            REL_WRITER_VALUES_FIELD.setAccessible(true);
        } catch (Exception e) {
            // TODO: Fallback to null?
            throw new RuntimeException("Failed to get RelWriterImpl.values field", e);
        }
    }

    private ExplainCreator(PhysicalRel root, Map<PhysicalRel, List<Integer>> idMap) {
        this.root = root;
        this.idMap = idMap;
    }

    public static QueryExplain explain(String sql, PhysicalRel root) {
        NodeIdVisitor idVisitor = new NodeIdVisitor();
        root.visit(idVisitor);
        Map<PhysicalRel, List<Integer>> idMap = idVisitor.getIdMap();

        List<QueryExplainElement> elements = new ExplainCreator(root, idMap).collect();

        return new QueryExplain(sql, elements);
    }

    private List<QueryExplainElement> collect() {
        collectNode(root);

        return elements;
    }

    private void collectNode(PhysicalRel node) {
        elements.add(createElement(node));

        for (RelNode input : node.getInputs()) {
            level++;

            collectNode((PhysicalRel) input);

            level--;
        }
    }

    private QueryExplainElement createElement(PhysicalRel node) {
        HazelcastRelOptCluster cluster = node.getHazelcastCluster();

        Cost cost = (Cost) node.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery());

        int nodeId = pollId(node);

        return new QueryExplainElement(
            nodeId,
            node.getRelTypeName(),
            getExplain(node, nodeId),
            cost.getRowsInternal(),
            cost.getCpuInternal(),
            cost.getNetworkInternal(),
            level
        );
    }

    private int pollId(PhysicalRel node) {
        List<Integer> ids = idMap.get(node);

        assert ids != null;
        assert !ids.isEmpty();

        // We remove the last element here because we go top-down as opposed to visitor which collects IDs bottom-up.
        return ids.remove(ids.size() - 1);
    }

    private String getExplain(PhysicalRel node, int nodeId) {
        List<Pair<String, Object>> values = getExplainValues(node);

        StringBuilder res = new StringBuilder();

        res.append(node.getRelTypeName()).append("#").append(nodeId);

        if (!values.isEmpty()) {
            res.append("[");

            for (int i = 0; i < values.size(); i++) {
                if (i != 0) {
                    res.append(", ");
                }

                Pair<String, Object> value = values.get(i);

                res.append(value.left).append("=").append(value.right);
            }

            res.append("]");
        }

        return res.toString();
    }

    @SuppressWarnings("unchecked")
    private static List<Pair<String, Object>> getExplainValues(PhysicalRel node) {
        RelWriterImpl relWriter = new RelWriterImpl(new PrintWriter(new StringWriter()), SqlExplainLevel.NO_ATTRIBUTES, false);
        ((AbstractRelNode) node).explainTerms(relWriter);

        List<Pair<String, Object>> values;

        try {
            values = (List<Pair<String, Object>>) REL_WRITER_VALUES_FIELD.get(relWriter);
        } catch (ReflectiveOperationException e) {
            // TODO: Fallback to null?
            throw new RuntimeException("Failed to get RelWriterImpl.values", e);
        }

        List<Pair<String, Object>> res = new ArrayList<>(values.size());

        for (Pair<String, Object> value : values) {
            // Skip inputs.
            if (value.right instanceof RelNode) {
                continue;
            }

            res.add(value);
        }

        return res;
    }
}
