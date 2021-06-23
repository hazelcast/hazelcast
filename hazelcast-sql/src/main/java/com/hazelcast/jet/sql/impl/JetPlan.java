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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.sql.impl.parse.SqlAlterJob.AlterJobOperation;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.jet.sql.impl.schema.Mapping;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.optimizer.PlanCheckContext;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.PlanObjectKey;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableModify.Operation;

import java.security.Permission;
import java.util.List;
import java.util.Set;

abstract class JetPlan extends SqlPlan {

    protected JetPlan(PlanKey planKey) {
        super(planKey);
    }

    abstract SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout);

    static class CreateMappingPlan extends JetPlan {
        private final Mapping mapping;
        private final boolean replace;
        private final boolean ifNotExists;
        private final JetPlanExecutor planExecutor;

        CreateMappingPlan(
                PlanKey planKey,
                Mapping mapping,
                boolean replace,
                boolean ifNotExists,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("CREATE MAPPING", arguments);
            return planExecutor.execute(this);
        }
    }

    static class DropMappingPlan extends JetPlan {
        private final String name;
        private final boolean ifExists;
        private final JetPlanExecutor planExecutor;

        DropMappingPlan(
                PlanKey planKey,
                String name,
                boolean ifExists,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("DROP MAPPING", arguments);
            return planExecutor.execute(this);
        }
    }

    static class CreateJobPlan extends JetPlan {
        private final JobConfig jobConfig;
        private final boolean ifNotExists;
        private final DmlPlan dmlPlan;
        private final JetPlanExecutor planExecutor;

        CreateJobPlan(
                PlanKey planKey,
                JobConfig jobConfig,
                boolean ifNotExists,
                DmlPlan dmlPlan,
                JetPlanExecutor planExecutor
        ) {
            super(planKey);

            assert dmlPlan.operation == Operation.INSERT : dmlPlan.operation;
            this.jobConfig = jobConfig;
            this.ifNotExists = ifNotExists;
            this.dmlPlan = dmlPlan;
            this.planExecutor = planExecutor;
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
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            return planExecutor.execute(this, arguments);
        }
    }

    static class AlterJobPlan extends JetPlan {
        private final String jobName;
        private final AlterJobOperation operation;
        private final JetPlanExecutor planExecutor;

        AlterJobPlan(
                PlanKey planKey,
                String jobName,
                AlterJobOperation operation,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("ALTER JOB", arguments);
            return planExecutor.execute(this);
        }
    }

    static class DropJobPlan extends JetPlan {
        private final String jobName;
        private final boolean ifExists;
        private final String withSnapshotName;
        private final JetPlanExecutor planExecutor;

        DropJobPlan(
                PlanKey planKey,
                String jobName,
                boolean ifExists,
                String withSnapshotName,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("DROP JOB", arguments);
            return planExecutor.execute(this);
        }
    }

    static class CreateSnapshotPlan extends JetPlan {
        private final String snapshotName;
        private final String jobName;
        private final JetPlanExecutor planExecutor;

        CreateSnapshotPlan(
                PlanKey planKey,
                String snapshotName,
                String jobName,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("CREATE SNAPSHOT", arguments);
            return planExecutor.execute(this);
        }
    }

    static class DropSnapshotPlan extends JetPlan {
        private final String snapshotName;
        private final boolean ifExists;
        private final JetPlanExecutor planExecutor;

        DropSnapshotPlan(
                PlanKey planKey,
                String snapshotName,
                boolean ifExists,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("DROP SNAPSHOT", arguments);
            return planExecutor.execute(this);
        }
    }

    static class ShowStatementPlan extends JetPlan {
        private final ShowStatementTarget showTarget;
        private final JetPlanExecutor planExecutor;

        ShowStatementPlan(
                PlanKey planKey,
                ShowStatementTarget showTarget,
                JetPlanExecutor planExecutor
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
        public boolean isPlanValid(PlanCheckContext context) {
            return true;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            JetPlan.ensureNoArguments("SHOW " + showTarget, arguments);
            return planExecutor.execute(this);
        }
    }

    static class SelectPlan extends JetPlan {
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final DAG dag;
        private final boolean isStreaming;
        private final SqlRowMetadata rowMetadata;
        private final JetPlanExecutor planExecutor;
        private final List<Permission> permissions;

        SelectPlan(
                PlanKey planKey,
                QueryParameterMetadata parameterMetadata,
                Set<PlanObjectKey> objectKeys,
                DAG dag,
                boolean isStreaming,
                SqlRowMetadata rowMetadata,
                JetPlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.objectKeys = objectKeys;
            this.parameterMetadata = parameterMetadata;
            this.dag = dag;
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
            for (Permission permission : permissions) {
                context.checkPermission(permission);
            }
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            return planExecutor.execute(this, queryId, arguments, timeout);
        }
    }

    private static void ensureNoArguments(String name, List<Object> arguments) {
        if (!arguments.isEmpty()) {
            throw QueryException.error(name + " does not support dynamic parameters");
        }
    }

    static class DmlPlan extends JetPlan {
        private final TableModify.Operation operation;
        private final Set<PlanObjectKey> objectKeys;
        private final QueryParameterMetadata parameterMetadata;
        private final DAG dag;
        private final JetPlanExecutor planExecutor;
        private final List<Permission> permissions;

        DmlPlan(
                TableModify.Operation operation,
                PlanKey planKey,
                QueryParameterMetadata parameterMetadata,
                Set<PlanObjectKey> objectKeys,
                DAG dag,
                JetPlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(planKey);

            this.operation = operation;
            this.objectKeys = objectKeys;
            this.parameterMetadata = parameterMetadata;
            this.dag = dag;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        public Operation getOperation() {
            return operation;
        }

        QueryParameterMetadata getParameterMetadata() {
            return parameterMetadata;
        }

        DAG getDag() {
            return dag;
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
            for (Permission permission : permissions) {
                context.checkPermission(permission);
            }
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId, List<Object> arguments, Long timeout) {
            return planExecutor.execute(this, queryId, arguments, timeout);
        }
    }
}
