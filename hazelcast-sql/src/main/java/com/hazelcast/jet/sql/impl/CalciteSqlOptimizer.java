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

package com.hazelcast.jet.sql.impl;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.AlterJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateViewPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DmlPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropViewPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapDeletePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapInsertPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapSelectPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapSinkPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapUpdatePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.SelectPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.connector.SqlConnectorCache;
import com.hazelcast.jet.sql.impl.connector.map.MetadataResolver;
import com.hazelcast.jet.sql.impl.connector.virtual.ViewTable;
import com.hazelcast.jet.sql.impl.opt.Conventions;
import com.hazelcast.jet.sql.impl.opt.OptUtils;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRel;
import com.hazelcast.jet.sql.impl.opt.logical.LogicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.CreateDagVisitor;
import com.hazelcast.jet.sql.impl.opt.physical.DeleteByKeyMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.InsertMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRules;
import com.hazelcast.jet.sql.impl.opt.physical.RootRel;
import com.hazelcast.jet.sql.impl.opt.physical.SelectByKeyMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.SinkMapPhysicalRel;
import com.hazelcast.jet.sql.impl.opt.physical.UpdateByKeyMapPhysicalRel;
import com.hazelcast.jet.sql.impl.parse.QueryConvertResult;
import com.hazelcast.jet.sql.impl.parse.QueryParseResult;
import com.hazelcast.jet.sql.impl.parse.SqlAlterJob;
import com.hazelcast.jet.sql.impl.parse.SqlCreateIndex;
import com.hazelcast.jet.sql.impl.parse.SqlCreateJob;
import com.hazelcast.jet.sql.impl.parse.SqlCreateMapping;
import com.hazelcast.jet.sql.impl.parse.SqlCreateSnapshot;
import com.hazelcast.jet.sql.impl.parse.SqlCreateView;
import com.hazelcast.jet.sql.impl.parse.SqlDropIndex;
import com.hazelcast.jet.sql.impl.parse.SqlDropJob;
import com.hazelcast.jet.sql.impl.parse.SqlDropMapping;
import com.hazelcast.jet.sql.impl.parse.SqlDropSnapshot;
import com.hazelcast.jet.sql.impl.parse.SqlDropView;
import com.hazelcast.jet.sql.impl.parse.SqlExplainStatement;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement;
import com.hazelcast.jet.sql.impl.schema.HazelcastTable;
import com.hazelcast.jet.sql.impl.schema.TableResolverImpl;
import com.hazelcast.jet.sql.impl.schema.TablesStorage;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.IMapResolver;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.state.QueryResultRegistry;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.util.SqlString;

import javax.annotation.Nullable;
import java.security.Permission;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateIndexPlan;
import static com.hazelcast.jet.sql.impl.SqlPlanImpl.DropIndexPlan;
import static com.hazelcast.jet.sql.impl.SqlPlanImpl.ExplainStatementPlan;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

/**
 * SQL optimizer based on Apache Calcite.
 * <p>
 * After parsing and initial sql-to-rel conversion is finished, all relational nodes start with {@link Convention#NONE}
 * convention. Such nodes are typically referred as "abstract" in Apache Calcite, because they do not have any physical
 * properties.
 * <p>
 * The optimization process is split into two phases - logical and physical. During logical planning we normalize abstract
 * nodes and convert them to nodes with {@link Conventions#LOGICAL} convention. These new nodes are Hazelcast-specific
 * and hence may have additional properties. For example, at this stage we do filter pushdowns, introduce constrained scans,
 * etc.
 * <p>
 * During physical planning we look for specific physical implementations of logical nodes. Implementation nodes have
 * {@link Conventions#PHYSICAL} convention. The process contains the following fundamental steps:
 * <ul>
 *     <li>Choosing proper access methods for scan (normal scan, index scan, etc)</li>
 *     <li>Propagating physical properties from children nodes to their parents</li>
 *     <li>Choosing proper implementations of parent operators based on physical properties of children
 *     (local vs. distributed sorting, blocking vs. streaming aggregation, hash join vs. merge join, etc.)</li>
 *     <li>Enforcing exchange operators when data movement is necessary</li>
 * </ul>
 * <p>
 * Physical optimization stage uses {@link VolcanoPlanner}. This is a rule-based optimizer. However it doesn't share any
 * architectural traits with EXODUS/Volcano/Cascades papers, except for the rule-based nature. In classical Cascades algorithm
 * [1], the optimization process is performed in a top-down style. Parent operator may request implementations of children
 * operators with specific properties. This is not possible in {@code VolcanoPlanner}. Instead, in this planner the rules are
 * fired in effectively uncontrollable fashion, thus making propagation of physical properties difficult. To overcome this
 * problem we use several techniques that helps us emulate at least some parts of Cascades-style optimization.
 * <p>
 * First, {@link Conventions#PHYSICAL} convention overrides {@link Convention#canConvertConvention(Convention)} and
 * {@link Convention#useAbstractConvertersForConversion(RelTraitSet, RelTraitSet)} methods. Their implementations ensure that
 * whenever a new child node with {@code PHYSICAL} convention is created, the rule of the parent {@code LOGICAL} nodes
 * will be re-scheduled. Second, physical rules for {@code LOGICAL} nodes iterate over concrete physical implementations of
 * inputs and convert logical nodes to physical nodes with proper traits. Combined, these techniques ensure complete exploration
 * of a search space and proper propagation of physical properties from child nodes to parent nodes. The downside is that
 * the same rule on the same node could be fired multiple times, thus increase the optimization time.
 * <p>
 * For example, consider the following logical tree:
 * <pre>
 * LogicalFilter
 *   LogicalScan
 * </pre>
 * By default Apache Calcite will fire a rule on the logical filter first. But at this point we do not know the physical
 * properties of {@code LogicalScan} implementations, since they are not produced yet. As a result, we do not know what
 * physical properties should be set to the to-be-created {@code PhysicalFilter}. Then Apache Calcite will optimize
 * {@code LogicalScan}, producing physical implementations. However, by default these new physical implementations will not
 * re-trigger optimization of {@code LogicalFilter}. The result of the optimization will be:
 * <pre>
 * [LogicalFilter, PhysicalFilter(???)]
 *   [LogicalScan, PhysicalScan(PARTITIONED), PhysicalIndexScan(PARTITIONED, a ASC)]
 * </pre>
 * Notice how we failed to propagate important physical properties to the {@code PhysicalFilter}.
 * <p>
 * With the above-described techniques we force Apache Calcite to re-optimize the logical parent after a new physical child
 * has been created. This way we are able to pull-up physical properties. The result of the optimization will be:
 * <pre>
 * [LogicalFilter, PhysicalFilter(PARTITIONED), PhysicalFilter(PARTITIONED, a ASC)]
 *   [LogicalScan, PhysicalScan(PARTITIONED), PhysicalIndexScan(PARTITIONED, a ASC)]
 * </pre>
 * <p>
 * [1] Efficiency In The Columbia Database Query Optimizer (1998), chapters 2 and 3
 */
public class CalciteSqlOptimizer implements SqlOptimizer {

    private final NodeEngine nodeEngine;

    private final IMapResolver iMapResolver;
    private final List<TableResolver> tableResolvers;
    private final PlanExecutor planExecutor;

    private final ILogger logger;

    public CalciteSqlOptimizer(NodeEngine nodeEngine, QueryResultRegistry resultRegistry) {
        this.nodeEngine = nodeEngine;

        this.iMapResolver = new MetadataResolver(nodeEngine);

        TableResolverImpl tableResolverImpl = mappingCatalog(nodeEngine);
        this.tableResolvers = singletonList(tableResolverImpl);
        this.planExecutor = new PlanExecutor(tableResolverImpl, nodeEngine.getHazelcastInstance(), resultRegistry);

        this.logger = nodeEngine.getLogger(getClass());
    }

    private static TableResolverImpl mappingCatalog(NodeEngine nodeEngine) {
        TablesStorage tablesStorage = new TablesStorage(nodeEngine);
        SqlConnectorCache connectorCache = new SqlConnectorCache(nodeEngine);
        return new TableResolverImpl(nodeEngine, tablesStorage, connectorCache);
    }

    @Nullable
    @Override
    public String mappingDdl(String name) {
        Mapping mapping = iMapResolver.resolve(name);
        return mapping != null ? SqlCreateMapping.unparse(mapping) : null;
    }

    @Override
    public List<TableResolver> tableResolvers() {
        return tableResolvers;
    }

    @Override
    public SqlPlan prepare(OptimizationTask task) {
        // 1. Prepare context.
        int memberCount = nodeEngine.getClusterService().getSize(MemberSelectors.DATA_MEMBER_SELECTOR);

        OptimizerContext context = OptimizerContext.create(
                task.getSchema(),
                task.getSearchPaths(),
                task.getArguments(),
                memberCount,
                iMapResolver);

        try {
            OptimizerContext.setThreadContext(context);

            // 2. Parse SQL string and validate it.
            QueryParseResult parseResult = context.parse(task.getSql());

            // 3. Create plan.
            return createPlan(task, parseResult, context);
        } finally {
            OptimizerContext.setThreadContext(null);
        }
    }

    @SuppressWarnings("checkstyle:returncount")
    private SqlPlan createPlan(
            OptimizationTask task,
            QueryParseResult parseResult,
            OptimizerContext context
    ) {
        // TODO [sasha] : refactor this.
        SqlNode node = parseResult.getNode();

        PlanKey planKey = new PlanKey(task.getSearchPaths(), task.getSql());
        if (node instanceof SqlCreateMapping) {
            return toCreateMappingPlan(planKey, (SqlCreateMapping) node);
        } else if (node instanceof SqlDropMapping) {
            return toDropMappingPlan(planKey, (SqlDropMapping) node);
        } else if (node instanceof SqlCreateIndex) {
            return toCreateIndexPlan(planKey, (SqlCreateIndex) node);
        } else if (node instanceof SqlDropIndex) {
            return toDropIndexPlan(planKey, (SqlDropIndex) node);
        } else if (node instanceof SqlCreateJob) {
            return toCreateJobPlan(planKey, parseResult, context, task.getSql());
        } else if (node instanceof SqlAlterJob) {
            return toAlterJobPlan(planKey, (SqlAlterJob) node);
        } else if (node instanceof SqlDropJob) {
            return toDropJobPlan(planKey, (SqlDropJob) node);
        } else if (node instanceof SqlCreateSnapshot) {
            return toCreateSnapshotPlan(planKey, (SqlCreateSnapshot) node);
        } else if (node instanceof SqlDropSnapshot) {
            return toDropSnapshotPlan(planKey, (SqlDropSnapshot) node);
        } else if (node instanceof SqlCreateView) {
            return toCreateViewPlan(planKey, context, (SqlCreateView) node);
        } else if (node instanceof SqlDropView) {
            return toDropViewPlan(planKey, (SqlDropView) node);
        } else if (node instanceof SqlShowStatement) {
            return toShowStatementPlan(planKey, (SqlShowStatement) node);
        } else if (node instanceof SqlExplainStatement) {
            return toExplainStatementPlan(planKey, context, parseResult);
        } else {
            QueryConvertResult convertResult = context.convert(parseResult.getNode());
            return toPlan(
                    planKey,
                    parseResult.getParameterMetadata(),
                    convertResult.getRel(),
                    convertResult.getFieldNames(),
                    context,
                    false,
                    task.getSql());
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

    private SqlPlan toCreateIndexPlan(PlanKey planKey, SqlCreateIndex sqlCreateIndex) {
        return new CreateIndexPlan(
                planKey,
                sqlCreateIndex.indexName(),
                sqlCreateIndex.mapName(),
                sqlCreateIndex.type(),
                sqlCreateIndex.columns(),
                sqlCreateIndex.options(),
                sqlCreateIndex.ifNotExists(),
                planExecutor
        );
    }

    private SqlPlan toDropIndexPlan(PlanKey planKey, SqlDropIndex sqlDropIndex) {
        return new DropIndexPlan(planKey, sqlDropIndex.indexName(), sqlDropIndex.ifExists(), planExecutor);
    }

    private SqlPlan toCreateJobPlan(PlanKey planKey, QueryParseResult parseResult, OptimizerContext context, String query) {
        SqlCreateJob sqlCreateJob = (SqlCreateJob) parseResult.getNode();
        SqlNode source = sqlCreateJob.dmlStatement();

        QueryParseResult dmlParseResult = new QueryParseResult(source, parseResult.getParameterMetadata());
        QueryConvertResult dmlConvertedResult = context.convert(dmlParseResult.getNode());
        boolean infiniteRows = OptUtils.isUnbounded(dmlConvertedResult.getRel());
        SqlPlanImpl dmlPlan = toPlan(
                null,
                parseResult.getParameterMetadata(),
                dmlConvertedResult.getRel(),
                dmlConvertedResult.getFieldNames(),
                context,
                true,
                query);
        assert dmlPlan instanceof DmlPlan && ((DmlPlan) dmlPlan).getOperation() == Operation.INSERT;

        return new CreateJobPlan(
                planKey,
                sqlCreateJob.jobConfig(),
                sqlCreateJob.ifNotExists(),
                (DmlPlan) dmlPlan,
                query,
                infiniteRows,
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

    private SqlPlan toCreateViewPlan(PlanKey planKey, OptimizerContext context, SqlCreateView sqlNode) {
        SqlString sqlString = sqlNode.getQuery().toSqlString(PostgresqlSqlDialect.DEFAULT);
        String sql = sqlString.getSql();
        boolean replace = sqlNode.getReplace();
        boolean ifNotExists = sqlNode.ifNotExists;

        return new CreateViewPlan(
                planKey,
                context,
                sqlNode.name(),
                sql,
                replace,
                ifNotExists,
                planExecutor
        );
    }

    private SqlPlan toDropViewPlan(PlanKey planKey, SqlDropView sqlNode) {
        return new DropViewPlan(planKey, sqlNode.viewName(), sqlNode.ifExists(), planExecutor);
    }

    private SqlPlan toShowStatementPlan(PlanKey planKey, SqlShowStatement sqlNode) {
        return new ShowStatementPlan(planKey, sqlNode.getTarget(), planExecutor);
    }

    private SqlPlan toExplainStatementPlan(
            PlanKey planKey,
            OptimizerContext context,
            QueryParseResult parseResult
    ) {
        SqlNode node = parseResult.getNode();
        assert node instanceof SqlExplainStatement;
        QueryConvertResult convertResult = context.convert(((SqlExplainStatement) node).getExplicandum());
        PhysicalRel physicalRel = optimize(
                parseResult.getParameterMetadata(),
                convertResult.getRel(),
                context,
                false
        );

        return new ExplainStatementPlan(planKey, physicalRel, planExecutor);
    }

    private SqlPlanImpl toPlan(
            PlanKey planKey,
            QueryParameterMetadata parameterMetadata,
            RelNode rel,
            List<String> fieldNames,
            OptimizerContext context,
            boolean isCreateJob,
            String query
    ) {
        PhysicalRel physicalRel = optimize(parameterMetadata, rel, context, isCreateJob);

        List<Permission> permissions = extractPermissions(physicalRel);

        if (physicalRel instanceof SelectByKeyMapPhysicalRel) {
            assert !isCreateJob;
            SelectByKeyMapPhysicalRel select = (SelectByKeyMapPhysicalRel) physicalRel;
            SqlRowMetadata rowMetadata = createRowMetadata(
                    fieldNames,
                    physicalRel.schema(parameterMetadata).getTypes(),
                    rel.getRowType().getFieldList()
            );
            return new IMapSelectPlan(
                    planKey,
                    select.objectKey(),
                    parameterMetadata, select.mapName(),
                    select.keyCondition(parameterMetadata),
                    select.rowProjectorSupplier(parameterMetadata),
                    rowMetadata,
                    planExecutor,
                    permissions
            );
        } else if (physicalRel instanceof InsertMapPhysicalRel) {
            assert !isCreateJob;
            InsertMapPhysicalRel insert = (InsertMapPhysicalRel) physicalRel;
            return new IMapInsertPlan(
                    planKey,
                    insert.objectKey(),
                    parameterMetadata,
                    insert.mapName(),
                    insert.entriesFn(),
                    planExecutor,
                    permissions
            );
        } else if (physicalRel instanceof SinkMapPhysicalRel) {
            assert !isCreateJob;
            SinkMapPhysicalRel sink = (SinkMapPhysicalRel) physicalRel;
            return new IMapSinkPlan(
                    planKey,
                    sink.objectKey(),
                    parameterMetadata,
                    sink.mapName(),
                    sink.entriesFn(),
                    planExecutor,
                    permissions
            );
        } else if (physicalRel instanceof UpdateByKeyMapPhysicalRel) {
            assert !isCreateJob;
            UpdateByKeyMapPhysicalRel update = (UpdateByKeyMapPhysicalRel) physicalRel;
            return new IMapUpdatePlan(
                    planKey,
                    update.objectKey(),
                    parameterMetadata,
                    update.mapName(),
                    update.keyCondition(parameterMetadata),
                    update.updaterSupplier(parameterMetadata),
                    planExecutor,
                    permissions
            );
        } else if (physicalRel instanceof DeleteByKeyMapPhysicalRel) {
            assert !isCreateJob;
            DeleteByKeyMapPhysicalRel delete = (DeleteByKeyMapPhysicalRel) physicalRel;
            return new IMapDeletePlan(
                    planKey,
                    delete.objectKey(),
                    parameterMetadata,
                    delete.mapName(),
                    delete.keyCondition(parameterMetadata),
                    planExecutor,
                    permissions
            );
        } else if (physicalRel instanceof TableModify) {
            checkDmlOperationWithView(physicalRel);
            Operation operation = ((TableModify) physicalRel).getOperation();
            CreateDagVisitor visitor = traverseRel(physicalRel, parameterMetadata);
            return new DmlPlan(
                    operation,
                    planKey,
                    parameterMetadata,
                    visitor.getObjectKeys(),
                    visitor.getDag(),
                    query,
                    OptUtils.isUnbounded(physicalRel),
                    planExecutor,
                    permissions
            );
        } else {
            CreateDagVisitor visitor = traverseRel(new RootRel(physicalRel), parameterMetadata);
            SqlRowMetadata rowMetadata = createRowMetadata(
                    fieldNames,
                    physicalRel.schema(parameterMetadata).getTypes(),
                    rel.getRowType().getFieldList()
            );
            return new SelectPlan(
                    planKey,
                    parameterMetadata,
                    visitor.getObjectKeys(),
                    visitor.getDag(),
                    query,
                    OptUtils.isUnbounded(physicalRel),
                    rowMetadata,
                    planExecutor,
                    permissions
            );
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

    private PhysicalRel optimize(
            QueryParameterMetadata parameterMetadata,
            RelNode rel,
            OptimizerContext context,
            boolean isCreateJob
    ) {
        context.setParameterMetadata(parameterMetadata);
        context.setRequiresJob(isCreateJob);

        boolean fineLogOn = logger.isFineEnabled();
        if (fineLogOn) {
            logger.fine("Before logical opt:\n" + RelOptUtil.toString(rel));
        }
        LogicalRel logicalRel = optimizeLogical(context, rel);
        if (fineLogOn) {
            logger.fine("After logical opt:\n" + RelOptUtil.toString(logicalRel));
        }

        PhysicalRel physicalRel = optimizePhysical(context, logicalRel);
        if (fineLogOn) {
            logger.fine("After physical opt:\n" + RelOptUtil.toString(physicalRel));
        }
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

    private SqlRowMetadata createRowMetadata(
            List<String> columnNames,
            List<QueryDataType> columnTypes,
            List<RelDataTypeField> fields
    ) {
        assert columnNames.size() == columnTypes.size();
        assert columnTypes.size() == fields.size();

        List<SqlColumnMetadata> columns = new ArrayList<>(columnNames.size());
        for (int i = 0; i < columnNames.size(); i++) {
            SqlColumnMetadata column = QueryUtils.getColumnMetadata(
                    columnNames.get(i),
                    columnTypes.get(i),
                    fields.get(i).getType().isNullable()
            );
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
        visitor.optimizeFinishedDag();
        return visitor;
    }

    private void checkDmlOperationWithView(PhysicalRel rel) {
        HazelcastTable table = Objects.requireNonNull(rel.getTable()).unwrap(HazelcastTable.class);
        if (table.getTarget() instanceof ViewTable) {
            throw QueryException.error("DML operations not supported for views");
        }
    }
}
