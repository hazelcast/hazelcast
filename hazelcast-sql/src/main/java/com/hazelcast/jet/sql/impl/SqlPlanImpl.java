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

import com.hazelcast.config.IndexType;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.connector.map.UpdatingEntryProcessor;
import com.hazelcast.jet.sql.impl.opt.physical.PhysicalRel;
import com.hazelcast.jet.sql.impl.parse.SqlAlterJob.AlterJobOperation;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.SqlPermission;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.optimizer.PlanCheckContext;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.schema.Mapping;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;

import java.security.Permission;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE_VIEW;
import static com.hazelcast.security.permission.ActionConstants.ACTION_DESTROY;
import static com.hazelcast.security.permission.ActionConstants.ACTION_DROP_VIEW;
import static com.hazelcast.security.permission.ActionConstants.ACTION_INDEX;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static com.hazelcast.security.permission.ActionConstants.ACTION_REMOVE;

abstract class SqlPlanImpl extends SqlPlan {

    protected SqlPlanImpl(PlanKey planKey) {
        super(planKey);
    }

    public boolean isPlanValid(PlanCheckContext context) {
        throw new UnsupportedOperationException(isCacheable()
                ? "override this method"
                : "method should not be called for non-cacheable plans");
    }

    protected void checkPermissions(SqlSecurityContext context, DAG dag) {
        if (!context.isSecurityEnabled()) {
            return;
        }
        for (Vertex vertex : dag) {
            Permission permission = vertex.getMetaSupplier().getRequiredPermission();
            if (permission != null) {
                context.checkPermission(permission);
            }
        }
    }

    @Override
    public void checkPermissions(SqlSecurityContext context) {
    }

    static class CreateMappingPlan extends SqlPlanImpl {
        private final Mapping mapping;
        private final boolean replace;
        private final boolean ifNotExists;
        private final PlanExecutor planExecutor;

        CreateMappingPlan(
                PlanKey planKey,
                Mapping mapping,
                boolean replace,
                boolean ifNotExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.mapping = mapping;
            this.replace = replace;
            this.ifNotExists = ifNotExists;
            this.planExecutor = planExecutor;
        }

        Mapping mapping() {
            return mapping;
        }

        boolean replace() {
            return replace;
        }

        boolean ifNotExists() {
            return ifNotExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new SqlPermission(mapping.name(), ACTION_CREATE));
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("CREATE MAPPING", arguments);
            SqlPlanImpl.ensureNoTimeout("CREATE MAPPING", timeout);
            return planExecutor.execute(this);
        }
    }

    static class DropMappingPlan extends SqlPlanImpl {
        private final String name;
        private final boolean ifExists;
        private final PlanExecutor planExecutor;

        DropMappingPlan(
                PlanKey planKey,
                String name,
                boolean ifExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.name = name;
            this.ifExists = ifExists;
            this.planExecutor = planExecutor;
        }

        String name() {
            return name;
        }

        boolean ifExists() {
            return ifExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new SqlPermission(name, ACTION_DESTROY));
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("DROP MAPPING", arguments);
            SqlPlanImpl.ensureNoTimeout("DROP MAPPING", timeout);
            return planExecutor.execute(this);
        }
    }

    static class CreateIndexPlan extends SqlPlanImpl {
        private final String name;
        private final String mapName;
        private final String[] attributes;
        private final Map<String, String> options;
        private final IndexType indexType;
        private final boolean ifNotExists;
        private final PlanExecutor planExecutor;

        CreateIndexPlan(
                PlanKey planKey,
                String name,
                String mapName,
                IndexType indexType,
                List<String> attributes,
                Map<String, String> options,
                boolean ifNotExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.name = name;
            this.mapName = mapName;
            this.indexType = indexType;
            this.attributes = attributes.toArray(new String[0]);
            this.options = options;
            this.ifNotExists = ifNotExists;
            this.planExecutor = planExecutor;
        }

        public String indexName() {
            return name;
        }

        public String mapName() {
            return mapName;
        }

        public String[] attributes() {
            return attributes;
        }

        public IndexType indexType() {
            return indexType;
        }

        public Map<String, String> options() {
            return options;
        }

        boolean ifNotExists() {
            return ifNotExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new SqlPermission(name, ACTION_INDEX));
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("CREATE INDEX", arguments);
            SqlPlanImpl.ensureNoTimeout("CREATE INDEX", timeout);
            return planExecutor.execute(this);
        }
    }

    static class DropIndexPlan extends SqlPlanImpl {
        private final String name;
        private final boolean ifExists;
        private final PlanExecutor planExecutor;

        DropIndexPlan(
                PlanKey planKey,
                String name,
                boolean ifExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.name = name;
            this.ifExists = ifExists;
            this.planExecutor = planExecutor;
        }

        String name() {
            return name;
        }

        boolean ifExists() {
            return ifExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new SqlPermission(name, ACTION_DESTROY));
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            throw QueryException.error("DROP INDEX is not supported.");
        }
    }

    static class CreateJobPlan extends SqlPlanImpl {
        private final JobConfig jobConfig;
        private final boolean ifNotExists;
        private final DmlPlan dmlPlan;
        private final String query;
        private final boolean infiniteRows;
        private final PlanExecutor planExecutor;

        CreateJobPlan(
                PlanKey planKey,
                JobConfig jobConfig,
                boolean ifNotExists,
                DmlPlan dmlPlan,
                String query,
                boolean infiniteRows,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            assert dmlPlan.operation == Operation.INSERT : dmlPlan.operation;
            this.jobConfig = jobConfig;
            this.ifNotExists = ifNotExists;
            this.dmlPlan = dmlPlan;
            this.query = query;
            this.infiniteRows = infiniteRows;
            this.planExecutor = planExecutor;
        }

        public boolean isInfiniteRows() {
            return infiniteRows;
        }

        public String getQuery() {
            return query;
        }

        JobConfig getJobConfig() {
            return jobConfig;
        }

        boolean isIfNotExists() {
            return ifNotExists;
        }

        DmlPlan getExecutionPlan() {
            return dmlPlan;
        }

        QueryParameterMetadata getParameterMetadata() {
            return dmlPlan.getParameterMetadata();
        }

        @Override
        public boolean isCacheable() {
            return dmlPlan.isCacheable();
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return dmlPlan.isPlanValid(context);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            dmlPlan.checkPermissions(context);
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoTimeout("CREATE JOB", timeout);
            return planExecutor.execute(this, arguments);
        }
    }

    static class AlterJobPlan extends SqlPlanImpl {
        private final String jobName;
        private final AlterJobOperation operation;
        private final PlanExecutor planExecutor;

        AlterJobPlan(
                PlanKey planKey,
                String jobName,
                AlterJobOperation operation,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.jobName = jobName;
            this.operation = operation;
            this.planExecutor = planExecutor;
        }

        String getJobName() {
            return jobName;
        }

        AlterJobOperation getOperation() {
            return operation;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("ALTER JOB", arguments);
            SqlPlanImpl.ensureNoTimeout("ALTER JOB", timeout);
            return planExecutor.execute(this);
        }
    }

    static class DropJobPlan extends SqlPlanImpl {
        private final String jobName;
        private final boolean ifExists;
        private final String withSnapshotName;
        private final PlanExecutor planExecutor;

        DropJobPlan(
                PlanKey planKey,
                String jobName,
                boolean ifExists,
                String withSnapshotName,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.jobName = jobName;
            this.ifExists = ifExists;
            this.withSnapshotName = withSnapshotName;
            this.planExecutor = planExecutor;
        }

        String getJobName() {
            return jobName;
        }

        boolean isIfExists() {
            return ifExists;
        }

        String getWithSnapshotName() {
            return withSnapshotName;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("DROP JOB", arguments);
            SqlPlanImpl.ensureNoTimeout("DROP JOB", timeout);
            return planExecutor.execute(this);
        }
    }

    static class CreateSnapshotPlan extends SqlPlanImpl {
        private final String snapshotName;
        private final String jobName;
        private final PlanExecutor planExecutor;

        CreateSnapshotPlan(
                PlanKey planKey,
                String snapshotName,
                String jobName,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.snapshotName = snapshotName;
            this.jobName = jobName;
            this.planExecutor = planExecutor;
        }

        String getSnapshotName() {
            return snapshotName;
        }

        String getJobName() {
            return jobName;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("CREATE SNAPSHOT", arguments);
            SqlPlanImpl.ensureNoTimeout("CREATE SNAPSHOT", timeout);
            return planExecutor.execute(this);
        }
    }

    static class DropSnapshotPlan extends SqlPlanImpl {
        private final String snapshotName;
        private final boolean ifExists;
        private final PlanExecutor planExecutor;

        DropSnapshotPlan(
                PlanKey planKey,
                String snapshotName,
                boolean ifExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.snapshotName = snapshotName;
            this.ifExists = ifExists;
            this.planExecutor = planExecutor;
        }

        String getSnapshotName() {
            return snapshotName;
        }

        boolean isIfExists() {
            return ifExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("DROP SNAPSHOT", arguments);
            SqlPlanImpl.ensureNoTimeout("DROP SNAPSHOT", timeout);
            return planExecutor.execute(this);
        }
    }

    static class CreateViewPlan extends SqlPlanImpl {
        private final OptimizerContext context;
        private final String viewName;
        private final String viewQuery;
        private final boolean replace;
        private final boolean ifNotExists;
        private final PlanExecutor planExecutor;

        CreateViewPlan(
                PlanKey planKey,
                final OptimizerContext context,
                String viewName,
                String viewQuery,
                boolean replace,
                boolean ifNotExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.context = context;
            this.viewName = viewName;
            this.viewQuery = viewQuery;
            this.replace = replace;
            this.ifNotExists = ifNotExists;
            this.planExecutor = planExecutor;
        }

        public OptimizerContext context() {
            return context;
        }

        public String viewName() {
            return viewName;
        }

        public String viewQuery() {
            return viewQuery;
        }

        boolean isReplace() {
            return replace;
        }

        public boolean ifNotExists() {
            return ifNotExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new SqlPermission(viewName, ACTION_CREATE_VIEW));
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("CREATE VIEW", arguments);
            SqlPlanImpl.ensureNoTimeout("CREATE VIEW", timeout);
            return planExecutor.execute(this);
        }
    }

    static class DropViewPlan extends SqlPlanImpl {
        private final String viewName;
        private final boolean ifExists;
        private final PlanExecutor planExecutor;

        DropViewPlan(
                PlanKey planKey,
                String viewName,
                boolean ifExists,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.viewName = viewName;
            this.ifExists = ifExists;
            this.planExecutor = planExecutor;
        }

        String viewName() {
            return viewName;
        }

        boolean isIfExists() {
            return ifExists;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new SqlPermission(viewName, ACTION_DROP_VIEW));
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("DROP VIEW", arguments);
            SqlPlanImpl.ensureNoTimeout("DROP VIEW", timeout);
            return planExecutor.execute(this);
        }
    }

    static class ShowStatementPlan extends SqlPlanImpl {
        private final ShowStatementTarget showTarget;
        private final PlanExecutor planExecutor;

        ShowStatementPlan(
                PlanKey planKey,
                ShowStatementTarget showTarget,
                PlanExecutor planExecutor
        ) {
            super(planKey);

            this.showTarget = showTarget;
            this.planExecutor = planExecutor;
        }

        ShowStatementTarget getShowTarget() {
            return showTarget;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoArguments("SHOW " + showTarget, arguments);
            SqlPlanImpl.ensureNoTimeout("SHOW " + showTarget, timeout);
            return planExecutor.execute(this);
        }
    }

    static class ExplainStatementPlan extends SqlPlanImpl {
        private final PhysicalRel rel;
        private final PlanExecutor planExecutor;

        ExplainStatementPlan(
                PlanKey planKey,
                PhysicalRel rel,
                PlanExecutor planExecutor
        ) {
            super(planKey);
            this.rel = rel;
            this.planExecutor = planExecutor;
        }

        public PhysicalRel getRel() {
            return rel;
        }

        @Override
        public boolean isCacheable() {
            return false;
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            SqlPlanImpl.ensureNoTimeout("EXPLAIN", timeout);
            return planExecutor.execute(this);
        }
    }

    static class SelectPlan extends SqlPlanImpl {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final DAG dag;
        private final String query;
        private final boolean isStreaming;
        private final SqlRowMetadata rowMetadata;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        SelectPlan(
                PlanKey planKey,
                QueryParameterMetadata parameterMetadata,
                Set<PlanObjectKey> objectKeys,
                DAG dag,
                String query,
                boolean isStreaming,
                SqlRowMetadata rowMetadata,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = objectKeys;
            this.parameterMetadata = parameterMetadata;
            this.dag = dag;
            this.query = query;
            this.isStreaming = isStreaming;
            this.rowMetadata = rowMetadata;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        QueryParameterMetadata getParameterMetadata() {
            return parameterMetadata;
        }

        DAG getDag() {
            return dag;
        }

        boolean isStreaming() {
            return isStreaming;
        }

        SqlRowMetadata getRowMetadata() {
            return rowMetadata;
        }

        public String getQuery() {
            return query;
        }

        @Override
        public boolean isCacheable() {
            return !objectKeys.contains(PlanObjectKey.NON_CACHEABLE_OBJECT_KEY);
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            checkPermissions(context, dag);
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, queryId, arguments, timeout);
        }
    }

    static class DmlPlan extends SqlPlanImpl {
        private final TableModify.Operation operation;
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final DAG dag;
        private final String query;
        private final boolean infiniteRows;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        DmlPlan(
                Operation operation,
                PlanKey planKey,
                QueryParameterMetadata parameterMetadata,
                Set<PlanObjectKey> objectKeys,
                DAG dag,
                String query,
                boolean infiniteRows,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.operation = operation;
            this.objectKeys = objectKeys;
            this.parameterMetadata = parameterMetadata;
            this.dag = dag;
            this.query = query;
            this.infiniteRows = infiniteRows;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        Operation getOperation() {
            return operation;
        }

        QueryParameterMetadata getParameterMetadata() {
            return parameterMetadata;
        }

        DAG getDag() {
            return dag;
        }

        public String getQuery() {
            return query;
        }

        public boolean isInfiniteRows() {
            return infiniteRows;
        }

        @Override
        public boolean isCacheable() {
            return !objectKeys.contains(PlanObjectKey.NON_CACHEABLE_OBJECT_KEY);
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            checkPermissions(context, dag);
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, queryId, arguments, timeout);
        }
    }

    static class IMapSelectPlan extends SqlPlanImpl {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final String mapName;
        private final Expression<?> keyCondition;
        private final KvRowProjector.Supplier rowProjectorSupplier;
        private final SqlRowMetadata rowMetadata;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        IMapSelectPlan(
                PlanKey planKey,
                PlanObjectKey objectKey,
                QueryParameterMetadata parameterMetadata,
                String mapName,
                Expression<?> keyCondition,
                KvRowProjector.Supplier rowProjectorSupplier,
                SqlRowMetadata rowMetadata,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = Collections.singleton(objectKey);
            this.parameterMetadata = parameterMetadata;
            this.mapName = mapName;
            this.keyCondition = keyCondition;
            this.rowProjectorSupplier = rowProjectorSupplier;
            this.rowMetadata = rowMetadata;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        QueryParameterMetadata parameterMetadata() {
            return parameterMetadata;
        }

        String mapName() {
            return mapName;
        }

        Expression<?> keyCondition() {
            return keyCondition;
        }

        KvRowProjector.Supplier rowProjectorSupplier() {
            return rowProjectorSupplier;
        }

        SqlRowMetadata rowMetadata() {
            return rowMetadata;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, queryId, arguments, timeout);
        }
    }

    static class IMapInsertPlan extends SqlPlanImpl {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final String mapName;
        private final Function<ExpressionEvalContext, List<Entry<Object, Object>>> entriesFn;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        IMapInsertPlan(
                PlanKey planKey,
                PlanObjectKey objectKey,
                QueryParameterMetadata parameterMetadata,
                String mapName,
                Function<ExpressionEvalContext, List<Entry<Object, Object>>> entriesFn,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = Collections.singleton(objectKey);
            this.parameterMetadata = parameterMetadata;
            this.mapName = mapName;
            this.entriesFn = entriesFn;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        QueryParameterMetadata parameterMetadata() {
            return parameterMetadata;
        }

        String mapName() {
            return mapName;
        }

        Function<ExpressionEvalContext, List<Entry<Object, Object>>> entriesFn() {
            return entriesFn;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new MapPermission(mapName, ACTION_CREATE, ACTION_PUT));
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, arguments, timeout);
        }
    }

    static class IMapSinkPlan extends SqlPlanImpl {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final String mapName;
        private final Function<ExpressionEvalContext, Map<Object, Object>> entriesFn;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        IMapSinkPlan(
                PlanKey planKey,
                PlanObjectKey objectKey,
                QueryParameterMetadata parameterMetadata,
                String mapName,
                Function<ExpressionEvalContext, Map<Object, Object>> entriesFn,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = Collections.singleton(objectKey);
            this.parameterMetadata = parameterMetadata;
            this.mapName = mapName;
            this.entriesFn = entriesFn;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        QueryParameterMetadata parameterMetadata() {
            return parameterMetadata;
        }

        String mapName() {
            return mapName;
        }

        Function<ExpressionEvalContext, Map<Object, Object>> entriesFn() {
            return entriesFn;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            context.checkPermission(new MapPermission(mapName, ACTION_CREATE, ACTION_PUT, ACTION_REMOVE));
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, arguments, timeout);
        }
    }

    static class IMapUpdatePlan extends SqlPlanImpl {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final String mapName;
        private final Expression<?> keyCondition;
        private final UpdatingEntryProcessor.Supplier updaterSupplier;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        IMapUpdatePlan(
                PlanKey planKey,
                PlanObjectKey objectKey,
                QueryParameterMetadata parameterMetadata,
                String mapName,
                Expression<?> keyCondition,
                UpdatingEntryProcessor.Supplier updaterSupplier,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = Collections.singleton(objectKey);
            this.parameterMetadata = parameterMetadata;
            this.mapName = mapName;
            this.keyCondition = keyCondition;
            this.updaterSupplier = updaterSupplier;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        QueryParameterMetadata parameterMetadata() {
            return parameterMetadata;
        }

        String mapName() {
            return mapName;
        }

        Expression<?> keyCondition() {
            return keyCondition;
        }

        UpdatingEntryProcessor.Supplier updaterSupplier() {
            return updaterSupplier;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            // We are checking ACTION_CREATE and ACTION_READ permissions to align with DmlPlan.
            context.checkPermission(new MapPermission(mapName, ACTION_CREATE, ACTION_READ, ACTION_PUT, ACTION_REMOVE));
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, arguments, timeout);
        }
    }

    static class IMapDeletePlan extends SqlPlanImpl {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final String mapName;
        private final Expression<?> keyCondition;
        private final PlanExecutor planExecutor;
        private final List<Permission> permissions;

        IMapDeletePlan(
                PlanKey planKey,
                PlanObjectKey objectKey,
                QueryParameterMetadata parameterMetadata,
                String mapName,
                Expression<?> keyCondition,
                PlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = Collections.singleton(objectKey);
            this.parameterMetadata = parameterMetadata;
            this.mapName = mapName;
            this.keyCondition = keyCondition;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        QueryParameterMetadata parameterMetadata() {
            return parameterMetadata;
        }

        String mapName() {
            return mapName;
        }

        Expression<?> keyCondition() {
            return keyCondition;
        }

        @Override
        public boolean isCacheable() {
            return true;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            // We are checking ACTION_CREATE and ACTION_READ permissions to align with DmlPlan.
            context.checkPermission(new MapPermission(mapName, ACTION_CREATE, ACTION_READ, ACTION_PUT, ACTION_REMOVE));
            permissions.forEach(context::checkPermission);
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, long timeout) {
            return planExecutor.execute(this, arguments, timeout);
        }
    }

    private static void ensureNoArguments(String name, List<Object> arguments) {
        if (!arguments.isEmpty()) {
            throw QueryException.error(name + " does not support dynamic parameters");
        }
    }

    private static void ensureNoTimeout(String name, long timeout) {
        if (timeout > 0) {
            throw QueryException.error(name + " does not support timeout");
        }
    }
}
