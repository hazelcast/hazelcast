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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.JetPlan.AlterJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ExecutionPlan;
import com.hazelcast.jet.sql.impl.calcite.parser.JetSqlParser;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.JetRootRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.parse.SqlAlterJob;
import com.hazelcast.jet.sql.impl.parse.SqlCreateJob;
import com.hazelcast.jet.sql.impl.parse.SqlCreateMapping;
import com.hazelcast.jet.sql.impl.parse.SqlCreateSnapshot;
import com.hazelcast.jet.sql.impl.parse.SqlDropJob;
import com.hazelcast.jet.sql.impl.parse.SqlDropMapping;
import com.hazelcast.jet.sql.impl.parse.SqlDropSnapshot;
import com.hazelcast.jet.sql.impl.schema.JetTable;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.jet.sql.impl.validate.JetSqlValidator;
import com.hazelcast.jet.sql.impl.validate.UnsupportedOperationVisitor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.calcite.OptimizerContext;
import com.hazelcast.sql.impl.calcite.SqlBackend;
import com.hazelcast.sql.impl.calcite.parse.QueryConvertResult;
import com.hazelcast.sql.impl.calcite.parse.QueryParseResult;
import com.hazelcast.sql.impl.calcite.validate.types.HazelcastTypeFactory;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserImplFactory;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.SqlToRelConverter.Config;

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
            SqlConformance sqlConformance
    ) {
        return new JetSqlValidator(catalogReader, typeFactory, sqlConformance);
    }

    @Override
    public SqlVisitor<Void> unsupportedOperationVisitor(CatalogReader catalogReader) {
        return UnsupportedOperationVisitor.INSTANCE;
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
    public SqlPlan createPlan(
            OptimizationTask task,
            QueryParseResult parseResult,
            OptimizerContext context
    ) {
        SqlNode node = parseResult.getNode();

        if (node instanceof SqlCreateMapping) {
            return toCreateMappingPlan((SqlCreateMapping) node);
        } else if (node instanceof SqlDropMapping) {
            return toDropMappingPlan((SqlDropMapping) node);
        } else if (node instanceof SqlCreateJob) {
            return toCreateJobPlan(parseResult, context);
        } else if (node instanceof SqlAlterJob) {
            return toAlterJobPlan((SqlAlterJob) node);
        } else if (node instanceof SqlDropJob) {
            return toDropJobPlan((SqlDropJob) node);
        } else if (node instanceof SqlCreateSnapshot) {
            return toCreateSnapshotPlan((SqlCreateSnapshot) node);
        } else if (node instanceof SqlDropSnapshot) {
            return toDropSnapshotPlan((SqlDropSnapshot) node);
        } else {
            QueryConvertResult convertResult = context.convert(parseResult);
            return toPlan(convertResult.getRel(), convertResult.getFieldNames(), context);
        }
    }

    private SqlPlan toCreateMappingPlan(SqlCreateMapping sqlCreateMapping) {
        List<MappingField> mappingFields = sqlCreateMapping.columns()
                .map(field -> new MappingField(field.name(), field.type(), field.externalName()))
                .collect(toList());
        Mapping mapping = new Mapping(
                sqlCreateMapping.nameWithoutSchema(),
                sqlCreateMapping.type(),
                mappingFields,
                sqlCreateMapping.options()
        );

        return new CreateMappingPlan(
                mapping,
                sqlCreateMapping.getReplace(),
                sqlCreateMapping.ifNotExists(),
                planExecutor
        );
    }

    private SqlPlan toDropMappingPlan(SqlDropMapping sqlDropMapping) {
        return new DropMappingPlan(sqlDropMapping.nameWithoutSchema(), sqlDropMapping.ifExists(), planExecutor);
    }

    private SqlPlan toCreateJobPlan(QueryParseResult parseResult, OptimizerContext context) {
        SqlCreateJob sqlCreateJob = (SqlCreateJob) parseResult.getNode();
        SqlNode source = sqlCreateJob.dmlStatement();

        QueryParseResult dmlParseResult =
                new QueryParseResult(source, parseResult.getParameterRowType(), parseResult.getValidator(), this);
        QueryConvertResult dmlConvertedResult = context.convert(dmlParseResult);
        ExecutionPlan dmlPlan = toPlan(dmlConvertedResult.getRel(), dmlConvertedResult.getFieldNames(), context);

        return new CreateJobPlan(
                sqlCreateJob.name(),
                sqlCreateJob.jobConfig(),
                sqlCreateJob.ifNotExists(),
                dmlPlan,
                planExecutor
        );
    }

    private SqlPlan toAlterJobPlan(SqlAlterJob sqlAlterJob) {
        return new AlterJobPlan(sqlAlterJob.name(), sqlAlterJob.getOperation(), planExecutor);
    }

    private SqlPlan toDropJobPlan(SqlDropJob sqlDropJob) {
        return new DropJobPlan(sqlDropJob.name(), sqlDropJob.ifExists(), sqlDropJob.withSnapshotName(), planExecutor);
    }

    private SqlPlan toCreateSnapshotPlan(SqlCreateSnapshot sqlNode) {
        return new CreateSnapshotPlan(sqlNode.getSnapshotName(), sqlNode.getJobName(), planExecutor);
    }

    private SqlPlan toDropSnapshotPlan(SqlDropSnapshot sqlNode) {
        return new DropSnapshotPlan(sqlNode.getSnapshotName(), sqlNode.isIfExists(), planExecutor);
    }

    private ExecutionPlan toPlan(RelNode rel, List<String> fieldNames, OptimizerContext context) {
        logger.fine("Before logical opt:\n" + RelOptUtil.toString(rel));
        LogicalRel logicalRel = optimizeLogical(context, rel);
        logger.fine("After logical opt:\n" + RelOptUtil.toString(logicalRel));
        PhysicalRel physicalRel = optimizePhysical(context, logicalRel);
        logger.fine("After physical opt:\n" + RelOptUtil.toString(physicalRel));

        boolean isStreaming = containsStreamSource(rel);
        boolean isInsert = physicalRel instanceof TableModify;

        if (isInsert) {
            DAG dag = createDag(physicalRel);
            return new ExecutionPlan(dag, isStreaming, true, null, null, planExecutor);
        } else {
            QueryId queryId = QueryId.create(nodeEngine.getLocalMember().getUuid());
            DAG dag = createDag(new JetRootRel(physicalRel, nodeEngine.getThisAddress(), queryId));
            SqlRowMetadata rowMetadata = createRowMetadata(fieldNames, physicalRel.schema().getTypes());
            return new ExecutionPlan(dag, isStreaming, false, queryId, rowMetadata, planExecutor);
        }
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

    /**
     * Goes over all tables of the rel and returns true if any of it is a stream source.
     */
    private boolean containsStreamSource(RelNode rel) {
        boolean[] containsStreamSource = {false};
        RelVisitor findStreamSourceVisitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    JetTable jetTable = node.getTable().unwrap(JetTable.class);
                    if (jetTable != null && jetTable.isStream()) {
                        containsStreamSource[0] = true;
                    }
                }
            }
        };
        findStreamSourceVisitor.go(rel);
        return containsStreamSource[0];
    }

    private SqlRowMetadata createRowMetadata(List<String> columnNames, List<QueryDataType> columnTypes) {
        assert columnNames.size() == columnTypes.size();

        List<SqlColumnMetadata> columns = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            SqlColumnMetadata column = QueryUtils.getColumnMetadata(columnNames.get(i), columnTypes.get(i));
            columns.add(column);
        }
        return new SqlRowMetadata(columns);
    }

    private DAG createDag(PhysicalRel physicalRel) {
        CreateDagVisitor visitor = new CreateDagVisitor(nodeEngine.getLocalMember().getAddress());
        physicalRel.accept(visitor);
        return visitor.getDag();
    }
}
