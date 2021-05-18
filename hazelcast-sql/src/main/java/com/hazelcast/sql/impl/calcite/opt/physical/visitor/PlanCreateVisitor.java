/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.opt.physical.FilterPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapIndexScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.MapScanPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ProjectPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.RootPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.SortPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.ValuesPhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.AbstractExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.RootExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.exchange.SortMergeExchangePhysicalRel;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.PlanFragmentMapping;
import com.hazelcast.sql.impl.plan.node.EmptyPlanNode;
import com.hazelcast.sql.impl.plan.node.FetchOffsetPlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.FetchPlanNode;
import com.hazelcast.sql.impl.plan.node.FilterPlanNode;
import com.hazelcast.sql.impl.plan.node.MapIndexScanPlanNode;
import com.hazelcast.sql.impl.plan.node.MapScanPlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNode;
import com.hazelcast.sql.impl.plan.node.PlanNodeFieldTypeProvider;
import com.hazelcast.sql.impl.plan.node.PlanNodeSchema;
import com.hazelcast.sql.impl.plan.node.ProjectPlanNode;
import com.hazelcast.sql.impl.plan.node.RootPlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceivePlanNode;
import com.hazelcast.sql.impl.plan.node.io.ReceiveSortMergePlanNode;
import com.hazelcast.sql.impl.plan.node.io.SendPlanNode;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;

import java.security.Permission;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
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
@SuppressWarnings({"rawtypes", "checkstyle:ClassDataAbstractionCoupling", "checkstyle:ClassFanOutComplexity"})
public class PlanCreateVisitor implements PhysicalRelVisitor {

    private final UUID localMemberId;
    private final Map<UUID, PartitionIdSet> partMap;
    private final Set<UUID> memberIds;
    private final Map<PhysicalRel, List<Integer>> relIdMap;
    private final PlanKey planKey;
    private final List<String> rootColumnNames;
    private final QueryParameterMetadata parameterMetadata;
    private final List<PlanNode> fragments = new ArrayList<>();
    private final List<PlanFragmentMapping> fragmentMappings = new ArrayList<>();
    private final List<Integer> fragmentOutboundEdge = new ArrayList<>();
    private final List<List<Integer>> fragmentInboundEdges = new ArrayList<>();
    private final Deque<PlanNode> upstreamNodes = new ArrayDeque<>();
    private int nextEdgeGenerator;
    private RootPhysicalRel rootPhysicalRel;
    private SqlRowMetadata rowMetadata;
    private final Set<PlanObjectKey> objectKeys = new HashSet<>();
    private final Set<String> mapNames = new HashSet<>();

    public PlanCreateVisitor(
            UUID localMemberId,
            Map<UUID, PartitionIdSet> partMap,
            Map<PhysicalRel, List<Integer>> relIdMap,
            PlanKey planKey,
            List<String> rootColumnNames,
            QueryParameterMetadata parameterMetadata
    ) {
        this.localMemberId = localMemberId;
        this.partMap = partMap;
        this.relIdMap = relIdMap;
        this.planKey = planKey;
        this.rootColumnNames = rootColumnNames;
        this.parameterMetadata = parameterMetadata;

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
        assert rowMetadata != null;

        List<Permission> permissions = new ArrayList<>();

        for (String mapName : mapNames) {
            permissions.add(new MapPermission(mapName, ActionConstants.ACTION_READ));
        }

        return new Plan(
                partMap,
                fragments,
                fragmentMappings,
                outboundEdgeMap,
                inboundEdgeMap,
                inboundEdgeMemberCountMap,
                rowMetadata,
                parameterMetadata,
                planKey,
                objectKeys,
                permissions
        );
    }

    @Override
    public void onRoot(RootPhysicalRel rel) {
        rootPhysicalRel = rel;
        List<Boolean> columnsNullable = new ArrayList<>();
        for (RelDataTypeField field : rel.getRowType().getFieldList()) {
            Boolean nullable = field.getType().isNullable();
            columnsNullable.add(nullable);
        }

        PlanNode upstreamNode = pollSingleUpstream();

        RootPlanNode rootNode = new RootPlanNode(
                pollId(rel),
                upstreamNode
        );

        rowMetadata = createRowMetadata(rootColumnNames, rootNode.getSchema().getTypes(), columnsNullable);

        addFragment(rootNode, new PlanFragmentMapping(Collections.singleton(localMemberId), false));
    }

    private static SqlRowMetadata createRowMetadata(
            List<String> columnNames,
            List<QueryDataType> columnTypes,
            List<Boolean> columnNullables) {
        assert columnNames.size() == columnTypes.size();
        assert columnNames.size() == columnNullables.size();

        List<SqlColumnMetadata> columns = new ArrayList<>(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            SqlColumnMetadata column = QueryUtils.getColumnMetadata(
                    columnNames.get(i),
                    columnTypes.get(i),
                    columnNullables.get(i)
            );
            columns.add(column);
        }

        return new SqlRowMetadata(columns);
    }

    @Override
    public void onMapScan(MapScanPhysicalRel rel) {
        AbstractMapTable table = rel.getMap();

        HazelcastTable hazelcastTable = rel.getTableUnwrapped();

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(table);

        MapScanPlanNode scanNode = new MapScanPlanNode(
                pollId(rel),
                table.getMapName(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                getScanFieldPaths(table),
                schemaBefore.getTypes(),
                hazelcastTable.getProjects(),
                convertFilter(schemaBefore, hazelcastTable.getFilter())
        );

        pushUpstream(scanNode);

        objectKeys.add(table.getObjectKey());
        mapNames.add(table.getMapName());
    }

    @Override
    public void onMapIndexScan(MapIndexScanPhysicalRel rel) {
        HazelcastTable hazelcastTable = rel.getTableUnwrapped();
        AbstractMapTable table = rel.getMap();

        PlanNodeSchema schemaBefore = getScanSchemaBeforeProject(table);

        MapIndexScanPlanNode scanNode = new MapIndexScanPlanNode(
                pollId(rel),
                table.getMapName(),
                table.getKeyDescriptor(),
                table.getValueDescriptor(),
                getScanFieldPaths(table),
                schemaBefore.getTypes(),
                hazelcastTable.getProjects(),
                rel.getIndex().getName(),
                rel.getIndex().getComponentsCount(),
                rel.getIndexFilter(),
                rel.getConverterTypes(),
                convertFilter(schemaBefore, rel.getRemainderExp()),
                rel.getAscs()
        );

        pushUpstream(scanNode);

        objectKeys.add(table.getObjectKey());
        mapNames.add(table.getMapName());
    }

    @Override
    public void onSort(SortPhysicalRel rel) {
        if (rel.requiresSort()) {
            StringBuilder msgBuilder = new StringBuilder("Cannot execute ORDER BY clause, because its input is not sorted. ");
            msgBuilder.append("Consider adding a SORTED index to the data source.");
            throw QueryException.error(msgBuilder.toString());
        }

        // Fetch/Offset only scenario
        if (rel.offset != null || rel.fetch != null) {
            PlanNode input = pollSingleUpstream();

            Expression fetch = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.fetch);
            Expression offset = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.offset);

            FetchPlanNode node = new FetchPlanNode(
                    pollId(rel),
                    input,
                    fetch,
                    offset
            );

            pushUpstream(node);
        }
    }

    @Override
    public void onRootExchange(RootExchangePhysicalRel rel) {
        // Get upstream node.
        PlanNode upstreamNode = pollSingleUpstream();

        // The receiver should process messages in order if the exchange is ordered.
        boolean ordered = !rel.getTraitSet().getTrait(RelCollationTraitDef.INSTANCE).getFieldCollations().isEmpty();

        // Create sender and push it as a fragment.
        int edge = nextEdge();
        int id = pollId(rel);

        SendPlanNode sendNode = new SendPlanNode(
                id,
                upstreamNode,
                edge
        );

        addFragment(sendNode, dataMemberMapping());

        // Create receiver.
        ReceivePlanNode receiveNode = new ReceivePlanNode(
                id,
                edge,
                ordered,
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

    @Override
    public void onSortMergeExchange(SortMergeExchangePhysicalRel rel) {
        PlanNode upstreamNode = pollSingleUpstream();

        // Create sender and push it as a fragment.
        int edge = nextEdge();

        int id = pollId(rel);

        SendPlanNode sendNode = new SendPlanNode(
                id,
                upstreamNode,
                edge
        );

        addFragment(sendNode, dataMemberMapping());

        List<RelFieldCollation> collations = rel.getCollation().getFieldCollations();
        int[] columnIndexes = new int[collations.size()];
        boolean[] ascs = new boolean[collations.size()];

        for (int i = 0; i < collations.size(); ++i) {
            RelFieldCollation collation = collations.get(i);
            RelFieldCollation.Direction direction = collation.getDirection();
            int idx = collation.getFieldIndex();

            columnIndexes[i] = idx;
            ascs[i] = !direction.isDescending();
        }

        Expression fetch = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.getFetch());
        Expression offset = convertExpression(FetchOffsetPlanNodeFieldTypeProvider.INSTANCE, rel.getOffset());

        // Create a receiver and push it to stack.
        ReceiveSortMergePlanNode receiveNode = new ReceiveSortMergePlanNode(
                id,
                edge,
                sendNode.getSchema().getTypes(),
                columnIndexes,
                ascs,
                fetch,
                offset
        );

        pushUpstream(receiveNode);
    }

    @Override
    public void onValues(ValuesPhysicalRel rel) {
        if (!rel.getTuples().isEmpty()) {
            throw QueryException.error("Non-empty VALUES are not supported");
        }

        QueryDataType[] fieldTypes = mapRowTypeToHazelcastTypes(rel.getRowType());

        EmptyPlanNode planNode = new EmptyPlanNode(
                pollId(rel),
                Arrays.asList(fieldTypes)
        );

        pushUpstream(planNode);
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
     * @param node    Node.
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

        RexToExpressionVisitor converter = new RexToExpressionVisitor(fieldTypeProvider, parameterMetadata);

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

    private static QueryDataType[] mapRowTypeToHazelcastTypes(RelDataType rowType) {
        List<RelDataTypeField> fields = rowType.getFieldList();

        QueryDataType[] mappedRowType = new QueryDataType[fields.size()];

        for (int i = 0; i < fields.size(); ++i) {
            mappedRowType[i] = HazelcastTypeUtils.toHazelcastType(fields.get(i).getType().getSqlTypeName());
        }

        return mappedRowType;
    }
}
