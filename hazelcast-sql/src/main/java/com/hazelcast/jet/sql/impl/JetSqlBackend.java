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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.sql.impl.JetPlan.AlterJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DmlPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.SelectPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.calcite.parser.JetSqlParser;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.opt.physical.JetRootRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.parse.SqlAlterJob;
import com.hazelcast.jet.sql.impl.parse.SqlCreateJob;
import com.hazelcast.jet.sql.impl.parse.SqlCreateMapping;
import com.hazelcast.jet.sql.impl.parse.SqlCreateSnapshot;
import com.hazelcast.jet.sql.impl.parse.SqlDropJob;
import com.hazelcast.jet.sql.impl.parse.SqlDropMapping;
import com.hazelcast.jet.sql.impl.parse.SqlDropSnapshot;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.jet.sql.impl.validate.JetSqlValidator;
import com.hazelcast.jet.sql.impl.validate.UnsupportedOperationVisitor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.schema.HazelcastTable;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;

import java.security.Permission;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

class JetSqlBackend implements SqlBackend {

    private final NodeEngine nodeEngine;
    private final JetPlanExecutor planExecutor;

    private final ILogger logger;

    JetSqlBackend(NodeEngine nodeEngine, JetPlanExecutor planExecutor) {
        this.nodeEngine = nodeEngine;
        this.planExecutor = planExecutor;

        this.logger = nodeEngine.getLogger(getClass());
    }

    @Override
    public SqlParserImplFactory parserFactory() {
        return JetSqlParser.FACTORY;
    }

    @Override
    public SqlValidator validator(
            CatalogReader catalogReader,
            HazelcastTypeFactory typeFactory,
            SqlConformance sqlConformance,
            List<Object> arguments
    ) {
        return new JetSqlValidator(catalogReader, typeFactory, sqlConformance, arguments);
    }

    @Override
    public SqlVisitor<Void> unsupportedOperationVisitor(CatalogReader catalogReader) {
        return new UnsupportedOperationVisitor();
    }

    @Override
    public SqlToRelConverter converter(
            ViewExpander viewExpander,
            SqlValidator sqlValidator,
            CatalogReader catalogReader,
            RelOptCluster relOptCluster,
            SqlRexConvertletTable sqlRexConvertletTable,
            Config config
    ) {
        return new JetSqlToRelConverter(
                viewExpander,
                sqlValidator,
                catalogReader,
                relOptCluster,
                sqlRexConvertletTable,
                config
        );
    }

    @Override
    @SuppressWarnings("checkstyle:ReturnCount")
    public SqlPlan createPlan(
            OptimizationTask task,
            QueryParseResult parseResult,
            OptimizerContext context
    ) {
        SqlNode node = parseResult.getNode();

        PlanKey planKey = new PlanKey(task.getSearchPaths(), task.getSql());
        if (node instanceof SqlCreateMapping) {
            return toCreateMappingPlan(planKey, (SqlCreateMapping) node);
        } else if (node instanceof SqlDropMapping) {
            return toDropMappingPlan(planKey, (SqlDropMapping) node);
        } else if (node instanceof SqlCreateJob) {
            return toCreateJobPlan(planKey, parseResult, context);
        } else if (node instanceof SqlAlterJob) {
            return toAlterJobPlan(planKey, (SqlAlterJob) node);
        } else if (node instanceof SqlDropJob) {
            return toDropJobPlan(planKey, (SqlDropJob) node);
        } else if (node instanceof SqlCreateSnapshot) {
            return toCreateSnapshotPlan(planKey, (SqlCreateSnapshot) node);
        } else if (node instanceof SqlDropSnapshot) {
            return toDropSnapshotPlan(planKey, (SqlDropSnapshot) node);
        } else if (node instanceof SqlShowStatement) {
            return toShowStatementPlan(planKey, (SqlShowStatement) node);
        } else {
            QueryConvertResult convertResult = context.convert(parseResult);
            return toPlan(
                    planKey,
                    parseResult.getParameterMetadata(),
                    convertResult.getRel(),
                    convertResult.getFieldNames(),
                    context,
                    parseResult.isInfiniteRows()
            );
        }
    }

    private SqlPlan toCreateMappingPlan(PlanKey planKey, SqlCreateMapping sqlCreateMapping) {
        List<MappingField> mappingFields = sqlCreateMapping.columns()
                .map(field -> new MappingField(field.name(), field.type(), field.externalName()))
                .collect(toList());
        Mapping mapping = new Mapping(
                sqlCreateMapping.nameWithoutSchema(),
                sqlCreateMapping.externalName(),
                sqlCreateMapping.type(),
                mappingFields,
                sqlCreateMapping.options()
        );

        return new CreateMappingPlan(
                planKey,
                mapping,
                sqlCreateMapping.getReplace(),
                sqlCreateMapping.ifNotExists(),
                planExecutor
        );
    }

    private SqlPlan toDropMappingPlan(PlanKey planKey, SqlDropMapping sqlDropMapping) {
        return new DropMappingPlan(planKey, sqlDropMapping.nameWithoutSchema(), sqlDropMapping.ifExists(), planExecutor);
    }

    private SqlPlan toCreateJobPlan(PlanKey planKey, QueryParseResult parseResult, OptimizerContext context) {
        SqlCreateJob sqlCreateJob = (SqlCreateJob) parseResult.getNode();
        SqlNode source = sqlCreateJob.dmlStatement();

        QueryParseResult dmlParseResult =
                new QueryParseResult(source, parseResult.getParameterMetadata(), parseResult.getValidator(), this, false);
        QueryConvertResult dmlConvertedResult = context.convert(dmlParseResult);
        JetPlan dmlPlan = toPlan(
                null,
                parseResult.getParameterMetadata(),
                dmlConvertedResult.getRel(),
                dmlConvertedResult.getFieldNames(),
                context,
                dmlParseResult.isInfiniteRows()
        );
        assert dmlPlan instanceof DmlPlan && ((DmlPlan) dmlPlan).getOperation() == Operation.INSERT;

        return new CreateJobPlan(
                planKey,
                sqlCreateJob.jobConfig(),
                sqlCreateJob.ifNotExists(),
                (DmlPlan) dmlPlan,
                planExecutor
        );
    }

    private SqlPlan toAlterJobPlan(PlanKey planKey, SqlAlterJob sqlAlterJob) {
        return new AlterJobPlan(planKey, sqlAlterJob.name(), sqlAlterJob.getOperation(), planExecutor);
    }

    private SqlPlan toDropJobPlan(PlanKey planKey, SqlDropJob sqlDropJob) {
        return new DropJobPlan(
                planKey,
                sqlDropJob.name(),
                sqlDropJob.ifExists(),
                sqlDropJob.withSnapshotName(),
                planExecutor
        );
    }

    private SqlPlan toCreateSnapshotPlan(PlanKey planKey, SqlCreateSnapshot sqlNode) {
        return new CreateSnapshotPlan(planKey, sqlNode.getSnapshotName(), sqlNode.getJobName(), planExecutor);
    }

    private SqlPlan toDropSnapshotPlan(PlanKey planKey, SqlDropSnapshot sqlNode) {
        return new DropSnapshotPlan(planKey, sqlNode.getSnapshotName(), sqlNode.isIfExists(), planExecutor);
    }

    private SqlPlan toShowStatementPlan(PlanKey planKey, SqlShowStatement sqlNode) {
        return new ShowStatementPlan(planKey, sqlNode.getTarget(), planExecutor);
    }

    private JetPlan toPlan(
            PlanKey planKey,
            QueryParameterMetadata parameterMetadata,
            RelNode rel,
            List<String> fieldNames,
            OptimizerContext context,
            boolean isInfiniteRows
    ) {
        PhysicalRel physicalRel = optimize(parameterMetadata, rel, context);

        Address localAddress = nodeEngine.getThisAddress();
        List<Permission> permissions = extractPermissions(physicalRel);

        if (physicalRel instanceof TableModify) {
            CreateDagVisitor visitor = traverseRel(physicalRel, parameterMetadata);
            Operation operation = ((TableModify) physicalRel).getOperation();
            return new DmlPlan(operation, planKey, parameterMetadata, visitor.getObjectKeys(), visitor.getDag(),
                    planExecutor, permissions);
        } else {
            CreateDagVisitor visitor = traverseRel(new JetRootRel(physicalRel, localAddress), parameterMetadata);
            SqlRowMetadata rowMetadata = createRowMetadata(fieldNames, physicalRel.schema(parameterMetadata).getTypes());
            return new SelectPlan(planKey, parameterMetadata, visitor.getObjectKeys(), visitor.getDag(), isInfiniteRows,
                    rowMetadata, planExecutor, permissions);
        }
    }

    private List<Permission> extractPermissions(PhysicalRel physicalRel) {
        List<Permission> permissions = new ArrayList<>();

        physicalRel.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                addPermissionForTable(scan.getTable(), ActionConstants.ACTION_READ);
                return super.visit(scan);
            }

            @Override
            public RelNode visit(RelNode other) {
                addPermissionForTable(other.getTable(), ActionConstants.ACTION_PUT);
                return super.visit(other);
            }

            private void addPermissionForTable(RelOptTable t, String action) {
                if (t == null) {
                    return;
                }
                HazelcastTable table = t.unwrap(HazelcastTable.class);
                if (table != null && table.getTarget() instanceof AbstractMapTable) {
                    String mapName = ((AbstractMapTable) table.getTarget()).getMapName();
                    permissions.add(new MapPermission(mapName, action));
                }
            }
        });

        return permissions;
    }

    private PhysicalRel optimize(QueryParameterMetadata parameterMetadata, RelNode rel, OptimizerContext context) {
        context.setParameterMetadata(parameterMetadata);

        logger.fine("Before logical opt:\n" + RelOptUtil.toString(rel));
        LogicalRel logicalRel = optimizeLogical(context, rel);
        logger.fine("After logical opt:\n" + RelOptUtil.toString(logicalRel));
        PhysicalRel physicalRel = optimizePhysical(context, logicalRel);
        logger.fine("After physical opt:\n" + RelOptUtil.toString(physicalRel));
        return physicalRel;
    }

    /**
     * Perform logical optimization.
     *
     * @param rel Original logical tree.
     * @return Optimized logical tree.
     */
    private LogicalRel optimizeLogical(OptimizerContext context, RelNode rel) {
        return (LogicalRel) context.optimize(
                rel,
                LogicalRules.getRuleSet(),
                OptUtils.toLogicalConvention(rel.getTraitSet())
        );
    }

    /**
     * Perform physical optimization.
     * This is where proper access methods and algorithms for joins and aggregations are chosen.
     *
     * @param rel Optimized logical tree.
     * @return Optimized physical tree.
     */
    private PhysicalRel optimizePhysical(OptimizerContext context, RelNode rel) {
        return (PhysicalRel) context.optimize(
                rel,
                PhysicalRules.getRuleSet(),
                OptUtils.toPhysicalConvention(rel.getTraitSet())
        );
    }

    private SqlRowMetadata createRowMetadata(List<String> columnNames, List<QueryDataType> columnTypes) {
        assert columnNames.size() == columnTypes.size();

        List<SqlColumnMetadata> columns = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            SqlColumnMetadata column = QueryUtils.getColumnMetadata(columnNames.get(i), columnTypes.get(i), true);
            columns.add(column);
        }
        return new SqlRowMetadata(columns);
    }

    private CreateDagVisitor traverseRel(
            PhysicalRel physicalRel,
            QueryParameterMetadata parameterMetadata
    ) {
        CreateDagVisitor visitor = new CreateDagVisitor(this.nodeEngine, parameterMetadata);
        physicalRel.accept(visitor);
        return visitor;
    }
}
