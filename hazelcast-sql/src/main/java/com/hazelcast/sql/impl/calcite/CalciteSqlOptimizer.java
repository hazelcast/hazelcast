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

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.partition.Partition;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.JetSqlBackend;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRules;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRules;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.SqlToQueryType;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.parse.SqlCreateExternalTable;
import com.hazelcast.sql.impl.calcite.parse.SqlDropExternalTable;
import com.hazelcast.sql.impl.calcite.parse.SqlOption;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.ExternalCatalog;
import com.hazelcast.sql.impl.schema.ExternalTable;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.SchemaPlan;
import com.hazelcast.sql.impl.schema.SchemaPlan.CreateExternalTablePlan;
import com.hazelcast.sql.impl.schema.SchemaPlan.RemoveExternalTablePlan;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTableResolver;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * Calcite-based SQL optimizer.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class CalciteSqlOptimizer implements SqlOptimizer {
    /** Node engine. */
    private final NodeEngine nodeEngine;

    /** Catalog. */
    private final ExternalCatalog catalog;

    /** Table resolvers used for schema resolution. */
    private final List<TableResolver> tableResolvers;

    private final JetSqlBackend jetSqlBackend;

    public CalciteSqlOptimizer(NodeEngine nodeEngine, JetSqlBackend jetSqlBackend) {
        this.nodeEngine = nodeEngine;
        this.catalog = new ExternalCatalog(nodeEngine);

        tableResolvers = createTableResolvers(catalog, nodeEngine);

        this.jetSqlBackend = jetSqlBackend;
    }

    @Override
    public SqlPlan prepare(OptimizationTask task) {
        // 1. Prepare context.
        int memberCount = nodeEngine.getClusterService().getSize(MemberSelectors.DATA_MEMBER_SELECTOR);

        OptimizerContext context = OptimizerContext.create(
            jetSqlBackend,
            tableResolvers,
            task.getSearchPaths(),
            memberCount
        );

        // 2. Parse SQL string and validate it.
        QueryParseResult parseResult = context.parse(task.getSql());

        if (parseResult.isDdl()) {
            return createSchemaPlan(parseResult.getNode());
        }

        // 3. Convert parse tree to relational tree.
        RelNode rel = context.convert(parseResult.getNode());

        // 4. Determine if Jet is needed to execute the query.
        boolean isImdg = isImdgOnly(rel);

        if (isImdg) {
            // 5. Perform optimization.
            PhysicalRel physicalRel = optimize(context, rel);

            // 6. Create plan.
            return createImdgPlan(task.getSql(), parseResult.getParameterRowType(), physicalRel);
        } else {
            return jetSqlBackend.optimizeAndCreatePlan(context, rel);
        }
    }

    private boolean isImdgOnly(RelNode rel) {
        if (jetSqlBackend == null) {
            // Jet not present on classpath - all queries are IMDG only
            return true;
        }

        boolean[] imdgOnly = {true};

        RelVisitor visitor = new RelVisitor() {
            public void visit(RelNode p, int ordinal, RelNode parent) {
                super.visit(p, ordinal, parent);
                // DML is only supported by Jet for now, even though only local maps are involved
                if (p instanceof TableModify) {
                    imdgOnly[0] = false;
                }

                RelOptTable table = p.getTable();
                if (table == null) {
                    return;
                }
                HazelcastTable table1 = table.unwrap(HazelcastTable.class);
                if (table1 == null) {
                    return;
                }
                // If there's any object other than IMap or ReplicatedMap involved, it runs on Jet
                if (!(table1.getTarget() instanceof AbstractMapTable)) {
                    imdgOnly[0] = false;
                }
            }
        };

        visitor.go(rel);
        return imdgOnly[0];
    }

    private SchemaPlan createSchemaPlan(SqlNode node) {
        if (node instanceof SqlCreateExternalTable) {
            return createCreateExternalTablePlan((SqlCreateExternalTable) node);
        } else if (node instanceof SqlDropExternalTable) {
            return createRemoveExternalTablePlan((SqlDropExternalTable) node);
        } else {
            throw new IllegalArgumentException("Unsupported SQL statement - " + node);
        }
    }

    private SchemaPlan createCreateExternalTablePlan(SqlCreateExternalTable sqlCreateTable) {
        List<ExternalField> externalFields = sqlCreateTable.columns()
                                                           .map(column -> new ExternalField(column.name(), column.type().type()))
                                                           .collect(toList());
        Map<String, String> options = sqlCreateTable.options()
                                                            .collect(toMap(SqlOption::key, SqlOption::value));
        ExternalTable schema = new ExternalTable(sqlCreateTable.name(), sqlCreateTable.type(), externalFields, options);

        return new CreateExternalTablePlan(catalog, schema, sqlCreateTable.getReplace(), sqlCreateTable.ifNotExists());
    }

    private SchemaPlan createRemoveExternalTablePlan(SqlDropExternalTable sqlDropTable) {
        return new RemoveExternalTablePlan(catalog, sqlDropTable.name(), sqlDropTable.ifExists());
    }

    private PhysicalRel optimize(OptimizerContext context, RelNode rel) {
        // Logical part.
        RelNode logicalRel = context.optimize(rel, LogicalRules.getRuleSet(), OptUtils.toLogicalConvention(rel.getTraitSet()));

        RootLogicalRel logicalRootRel = new RootLogicalRel(logicalRel.getCluster(), logicalRel.getTraitSet(), logicalRel);

        // Physical part.
        RelTraitSet physicalTraitSet = OptUtils.toPhysicalConvention(
            logicalRootRel.getTraitSet(),
            OptUtils.getDistributionDef(logicalRootRel).getTraitRoot()
        );

        return (PhysicalRel) context.optimize(logicalRootRel, PhysicalRules.getRuleSet(), physicalTraitSet);
    }

    /**
     * Create plan from physical rel.
     *
     * @param rel Rel.
     * @return Plan.
     */
    private Plan createImdgPlan(String sql, RelDataType parameterRowType, PhysicalRel rel) {
        // Get partition mapping.
        Collection<Partition> parts = nodeEngine.getHazelcastInstance().getPartitionService().getPartitions();

        int partCnt = parts.size();

        LinkedHashMap<UUID, PartitionIdSet> partMap = new LinkedHashMap<>();

        for (Partition part : parts) {
            UUID ownerId = part.getOwner().getUuid();

            partMap.computeIfAbsent(ownerId, (key) -> new PartitionIdSet(partCnt)).add(part.getPartitionId());
        }

        // Assign IDs to nodes.
        NodeIdVisitor idVisitor = new NodeIdVisitor();
        rel.visit(idVisitor);
        Map<PhysicalRel, List<Integer>> relIdMap = idVisitor.getIdMap();

        // Create the plan.
        QueryDataType[] mappedParameterRowType = SqlToQueryType.mapRowType(parameterRowType);
        QueryParameterMetadata parameterMetadata = new QueryParameterMetadata(mappedParameterRowType);

        PlanCreateVisitor visitor = new PlanCreateVisitor(
            nodeEngine.getLocalMember().getUuid(),
            partMap,
            relIdMap,
            sql,
            parameterMetadata
        );

        rel.visit(visitor);

        return visitor.getPlan();
    }

    private static List<TableResolver> createTableResolvers(ExternalCatalog catalog, NodeEngine nodeEngine) {
        List<TableResolver> res = new ArrayList<>(3);

        res.add(catalog);
        res.add(new PartitionedMapTableResolver(nodeEngine));
        res.add(new ReplicatedMapTableResolver(nodeEngine));

        // TODO: Add Jet resolvers

        return res;
    }
}
