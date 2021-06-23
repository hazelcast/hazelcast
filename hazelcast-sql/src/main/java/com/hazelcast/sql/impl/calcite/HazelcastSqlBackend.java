/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite;

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.opt.OptUtils;
import com.hazelcast.sql.impl.calcite.opt.logical.LogicalRules;
import com.hazelcast.sql.impl.calcite.opt.logical.RootLogicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRel;
import com.hazelcast.sql.impl.calcite.opt.physical.PhysicalRules;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.NodeIdVisitor;
import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PlanCreateVisitor;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.parse.UnsupportedOperationVisitor;
import com.hazelcast.sql.impl.calcite.validate.HazelcastSqlValidator;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;
import org.apache.calcite.sql2rel.StandardConvertletTable;

import java.util.List;
import java.util.Map;

/**
 * An IMDG specific {@link SqlBackend}.
 */
public class HazelcastSqlBackend implements SqlBackend {

    private final NodeEngine nodeEngine;

    public HazelcastSqlBackend(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @Override
    public SqlParserImplFactory parserFactory() {
        return null;
    }

    @Override
    public SqlValidator validator(
            CatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance conformance,
            List<Object> arguments
    ) {
        return new HazelcastSqlValidator(catalogReader, typeFactory, conformance, arguments);
    }

    @Override
    public SqlVisitor<Void> unsupportedOperationVisitor(CatalogReader catalogReader) {
        return new UnsupportedOperationVisitor(catalogReader);
    }

    @Override
    public SqlToRelConverter converter(
            ViewExpander viewExpander,
            SqlValidator validator,
            CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config
    ) {
        return new HazelcastSqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                config
        );
    }

    @Override
    public SqlPlan createPlan(OptimizationTask task, QueryParseResult parseResult, OptimizerContext context) {
        QueryConvertResult convertResult = context.convert(parseResult);

        PhysicalRel physicalRel = optimize(context, convertResult.getRel(), parseResult.getParameterMetadata());

        String sql = task.getSql();
        // Assign IDs to nodes.
        NodeIdVisitor idVisitor = new NodeIdVisitor();
        physicalRel.visit(idVisitor);
        Map<PhysicalRel, List<Integer>> relIdMap = idVisitor.getIdMap();

        // Create the plan.
        Member localMember = nodeEngine.getLocalMember();

        PlanCreateVisitor visitor = new PlanCreateVisitor(
                localMember.getUuid(),
                QueryUtils.createPartitionMap(nodeEngine, localMember.getVersion(), true),
                relIdMap,
                new PlanKey(task.getSearchPaths(), sql),
                convertResult.getFieldNames(),
                parseResult.getParameterMetadata()
        );

        physicalRel.visit(visitor);

        return visitor.getPlan();
    }

    private PhysicalRel optimize(
            OptimizerContext context,
            RelNode rel,
            QueryParameterMetadata parameterMetadata
    ) {
        // Make metadata available to planner
        context.setParameterMetadata(parameterMetadata);

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

}
