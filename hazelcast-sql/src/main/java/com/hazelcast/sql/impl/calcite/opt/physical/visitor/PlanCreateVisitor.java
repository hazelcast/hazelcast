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

package com.hazelcast.sql.impl.calcite.opt.physical.visitor;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.AbstractExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.RootSendPlanNode;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Visitor which produces executable query plan from a relational operator tree.
 * <p>
 * Plan is a sequence of fragments connected via send/receive operators. A fragment is a sequence of relational operators.
 * <p>
 * Most relational operators have one-to-one mapping to the appropriate {@link PlanNode}.
 * The exception is the family of {@link AbstractExchangePhysicalRel} operators. When an exchange is met, a new fragment is
 * created, and then exchange is converted into a pair of appropriate send/receive operators. Send operator is added to the
 * previous fragment, receive operator is a starting point for the new fragment.
 */
@SuppressWarnings("rawtypes")
public class PlanCreateVisitor implements PhysicalRelVisitor {
    /** ID of query coordinator. */
    private final UUID localMemberId;

    /** Partition mapping. */
    private final Map<UUID, PartitionIdSet> partMap;

    /** Participant IDs. */
    private final Set<UUID> memberIds;

    /** Rel ID map. */
    private final Map<PhysicalRel, List<Integer>> relIdMap;

    /** Prepared fragments. */
    private final List<PlanNode> fragments = new ArrayList<>();

    /** Fragment mappings. */
    private final List<PlanFragmentMapping> fragmentMappings = new ArrayList<>();

    /** Outbound edge of the fragment. */
    private final List<Integer> fragmentOutboundEdge = new ArrayList<>();

    /** Inbound edges of the fragment. */
    private final List<List<Integer>> fragmentInboundEdges = new ArrayList<>();

    /** Upstream nodes. Normally it is one node, except for multi-source operations (e.g. joins, sets, subqueries). */
    private final Deque<PlanNode> upstreamNodes = new ArrayDeque<>();

    /** ID of current edge. */
    private int nextEdgeGenerator;

    /** Root physical rel. */
    private RootPhysicalRel rootPhysicalRel;

    public PlanCreateVisitor(
        UUID localMemberId,
        Map<UUID, PartitionIdSet> partMap,
        Map<PhysicalRel, List<Integer>> relIdMap
    ) {
        this.localMemberId = localMemberId;
        this.partMap = partMap;
        this.relIdMap = relIdMap;

        memberIds = new HashSet<>(partMap.keySet());
    }

    public Plan getPlan() {
        // Calculate edges.
        Map<Integer, Integer> outboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMap = new HashMap<>();
        Map<Integer, Integer> inboundEdgeMemberCountMap = new HashMap<>();

        for (int i = 0; i < fragments.size(); i++) {
            PlanFragmentMapping fragmentMapping = fragmentMappings.get(i);
            Integer outboundEdge = fragmentOutboundEdge.get(i);
            List<Integer> inboundEdges = fragmentInboundEdges.get(i);

            if (outboundEdge != null) {
                outboundEdgeMap.put(outboundEdge, i);
            }

            if (inboundEdges != null) {
                for (Integer inboundEdge : inboundEdges) {
                    int count = fragmentMapping.isDataMembers() ? partMap.size() : fragmentMapping.getMemberIds().size();

                    inboundEdgeMap.put(inboundEdge, i);
                    inboundEdgeMemberCountMap.put(inboundEdge, count);
                }
            }
        }

        assert rootPhysicalRel != null;

        return new Plan(
            partMap,
            fragments,
            fragmentMappings,
            outboundEdgeMap,
            inboundEdgeMap,
            inboundEdgeMemberCountMap
        );
    }

    @Override
    public void onRoot(RootPhysicalRel rel) {
        rootPhysicalRel = rel;

        PlanNode upstreamNode = pollSingleUpstream();

        RootPlanNode rootNode = new RootPlanNode(
            pollId(rel),
            upstreamNode
        );

        addFragment(rootNode, new PlanFragmentMapping(Collections.singleton(localMemberId), false));
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        AbstractMapTable table = rel.getMap();

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(table);

        MapScanPlanNode scanNode = new MapScanPlanNode(
            pollId(rel),
            table.getName(),
            table.getKeyDescriptor(),
            table.getValueDescriptor(),
            getScanFieldPaths(table),
            schemaBefore.getTypes(),
            rel.getProjects(),
            convertFilter(schemaBefore, rel.getFilter())
        );

        pushUpstream(scanNode);
    }

    @Override
    public void onRootExchange(RootExchangePhysicalRel rel) {
        // Get upstream node.
        PlanNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        RootSendPlanNode sendNode = new RootSendPlanNode(
            id,
            upstreamNode,
            edge
        );

        addFragment(sendNode, dataMemberMapping());

        // Create receiver.
        ReceivePlanNode receiveNode = new ReceivePlanNode(
            id,
            edge,
            sendNode.getSchema().getTypes()
        );

        pushUpstream(receiveNode);
    }

    @Override
    public void onProject(ProjectPhysicalRel rel) {
        PlanNode upstreamNode = pollSingleUpstream();

        List<RexNode> projects = rel.getProjects();
        List<Expression> convertedProjects = new ArrayList<>(projects.size());

        for (RexNode project : projects) {
            Expression convertedProject = convertExpression(upstreamNode.getSchema(), project);

            convertedProjects.add(convertedProject);
        }

        ProjectPlanNode projectNode = new ProjectPlanNode(
            pollId(rel),
            upstreamNode,
            convertedProjects
        );

        pushUpstream(projectNode);
    }

    @Override
    public void onFilter(FilterPhysicalRel rel) {
        PlanNode upstreamNode = pollSingleUpstream();

        Expression<Boolean> filter = convertFilter(upstreamNode.getSchema(), rel.getCondition());

        FilterPlanNode filterNode = new FilterPlanNode(
            pollId(rel),
            upstreamNode,
            filter
        );

        pushUpstream(filterNode);
    }

    /**
     * Push node to upstream stack.
     *
     * @param node Node.
     */
    private void pushUpstream(PlanNode node) {
        upstreamNodes.addFirst(node);
    }

    /**
     * Poll an upstream node which is expected to be the only available in the stack.
     *
     * @return Upstream node.
     */
    private PlanNode pollSingleUpstream() {
        return upstreamNodes.pollFirst();
    }

    /**
     * Create new fragment and clear intermediate state.
     *
     * @param node Node.
     * @param mapping Fragment mapping mode.
     */
    private void addFragment(PlanNode node, PlanFragmentMapping mapping) {
        EdgeCollectorPlanNodeVisitor edgeVisitor = new EdgeCollectorPlanNodeVisitor();

        node.visit(edgeVisitor);

        fragments.add(node);
        fragmentMappings.add(mapping);
        fragmentOutboundEdge.add(edgeVisitor.getOutboundEdge());
        fragmentInboundEdges.add(edgeVisitor.getInboundEdges());
    }

    private int nextEdge() {
        return nextEdgeGenerator++;
    }

    private int pollId(PhysicalRel rel) {
        List<Integer> ids = relIdMap.get(rel);

        assert ids != null;
        assert !ids.isEmpty();

        return ids.remove(0);
    }

    @SuppressWarnings("unchecked")
    private Expression<Boolean> convertFilter(PlanNodeSchema schema, RexNode expression) {
        if (expression == null) {
            return null;
        }

        Expression convertedExpression = convertExpression(schema, expression);

        return (Expression<Boolean>) convertedExpression;
    }

    private Expression convertExpression(PlanNodeFieldTypeProvider fieldTypeProvider, RexNode expression) {
        if (expression == null) {
            return null;
        }

        RexToExpressionVisitor converter = new RexToExpressionVisitor(fieldTypeProvider);

        return expression.accept(converter);
    }

    private static PlanNodeSchema getScanSchemaBeforeProject(AbstractMapTable table) {
        List<QueryDataType> types = new ArrayList<>(table.getFieldCount());

        for (int i = 0; i < table.getFieldCount(); i++) {
            MapTableField field = table.getField(i);

            types.add(field.getType());
        }

        return new PlanNodeSchema(types);
    }

    private static List<QueryPath> getScanFieldPaths(AbstractMapTable table) {
        List<QueryPath> res = new ArrayList<>(table.getFieldCount());

        for (int i = 0; i < table.getFieldCount(); i++) {
            MapTableField field = table.getField(i);

            res.add(field.getPath());
        }

        return res;
    }

    private PlanFragmentMapping dataMemberMapping() {
        return new PlanFragmentMapping(memberIds, true);
    }
}
