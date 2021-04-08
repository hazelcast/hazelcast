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
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.cache.CacheablePlan;
import com.hazelcast.sql.impl.plan.cache.PlanCacheKey;
import com.hazelcast.sql.impl.plan.cache.PlanCheckContext;
import com.hazelcast.sql.impl.plan.cache.PlanObjectKey;
import com.hazelcast.sql.impl.security.SqlSecurityContext;

import java.security.Permission;
import java.util.List;
import java.util.Set;

interface JetPlan extends SqlPlan {

    SqlResult execute(QueryId queryId);

    static CreateMappingPlan toCreateMapping(
            Mapping mapping,
            boolean replace,
            boolean ifNotExists,
            JetPlanExecutor planExecutor
    ) {
        return new CreateMappingPlan(mapping, replace, ifNotExists, planExecutor);
    }

    static DropMappingPlan toDropMapping(
            String name,
            boolean ifExists,
            JetPlanExecutor planExecutor
    ) {
        return new DropMappingPlan(name, ifExists, planExecutor);
    }

    static CreateJobPlan toCreateJob(
            PlanCacheKey id,
            JobConfig jobConfig,
            boolean ifNotExists,
            SelectOrSinkPlan dmlPlan,
            JetPlanExecutor planExecutor
    ) {
        return dmlPlan instanceof CacheableJetPlan
                ? new CacheableCreateJobPlan(id, jobConfig, ifNotExists, (CacheableSelectOrSinkPlan) dmlPlan, planExecutor)
                : new NonCacheableCreateJobPlan(jobConfig, ifNotExists, dmlPlan, planExecutor);
    }

    static AlterJobPlan toAlterJob(
            String jobName,
            AlterJobOperation operation,
            JetPlanExecutor planExecutor
    ) {
        return new AlterJobPlan(jobName, operation, planExecutor);
    }

    static CreateSnapshotPlan toCreateSnapshot(
            String snapshotName,
            String jobName,
            JetPlanExecutor planExecutor
    ) {
        return new CreateSnapshotPlan(snapshotName, jobName, planExecutor);
    }

    static DropJobPlan toDropJob(
            String jobName,
            boolean ifExists,
            String withSnapshotName,
            JetPlanExecutor planExecutor
    ) {
        return new DropJobPlan(jobName, ifExists, withSnapshotName, planExecutor);
    }

    static DropSnapshotPlan toDropSnapshot(
            String snapshotName,
            boolean ifExists,
            JetPlanExecutor planExecutor
    ) {
        return new DropSnapshotPlan(snapshotName, ifExists, planExecutor);
    }

    static ShowStatementPlan toShowStatement(
            ShowStatementTarget showTarget,
            JetPlanExecutor planExecutor
    ) {
        return new ShowStatementPlan(showTarget, planExecutor);
    }

    @SuppressWarnings("checkstyle:ParameterNumber")
    static SelectOrSinkPlan toSelectOrSink(
            PlanCacheKey id,
            Set<PlanObjectKey> objectKeys,
            DAG dag,
            boolean isStreaming,
            boolean isInsert,
            SqlRowMetadata rowMetadata,
            JetPlanExecutor planExecutor,
            List<Permission> permissions
    ) {
        return objectKeys.contains(PlanObjectKey.NON_CACHEABLE_OBJECT_KEY)
                ? new NonCacheableSelectOrSinkPlan(dag, isStreaming, isInsert, rowMetadata, planExecutor, permissions)
                : new CacheableSelectOrSinkPlan(id, objectKeys, dag, isStreaming, isInsert, rowMetadata,
                planExecutor, permissions);
    }

    abstract class CacheableJetPlan implements JetPlan, CacheablePlan {

        private final PlanCacheKey id;

        private volatile long lastUsed;

        protected CacheableJetPlan(PlanCacheKey id) {
            this.id = id;
        }

        @Override
        public final PlanCacheKey getPlanKey() {
            assert id != null;

            return id;
        }

        @Override
        public final long getPlanLastUsed() {
            return lastUsed;
        }

        @Override
        public final void onPlanUsed() {
            lastUsed = System.currentTimeMillis();
        }
    }

    class CreateMappingPlan implements JetPlan {
        private final Mapping mapping;
        private final boolean replace;
        private final boolean ifNotExists;
        private final JetPlanExecutor planExecutor;

        private CreateMappingPlan(
                Mapping mapping,
                boolean replace,
                boolean ifNotExists,
                JetPlanExecutor planExecutor
        ) {
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
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class DropMappingPlan implements JetPlan {
        private final String name;
        private final boolean ifExists;
        private final JetPlanExecutor planExecutor;

        private DropMappingPlan(
                String name,
                boolean ifExists,
                JetPlanExecutor planExecutor
        ) {
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
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    interface CreateJobPlan extends JetPlan {
        JobConfig getJobConfig();

        boolean isIfNotExists();

        SelectOrSinkPlan getExecutionPlan();
    }

    class NonCacheableCreateJobPlan implements CreateJobPlan {
        private final JobConfig jobConfig;
        private final boolean ifNotExists;
        private final SelectOrSinkPlan dmlPlan;
        private final JetPlanExecutor planExecutor;

        private NonCacheableCreateJobPlan(
                JobConfig jobConfig,
                boolean ifNotExists,
                SelectOrSinkPlan dmlPlan,
                JetPlanExecutor planExecutor
        ) {
            this.jobConfig = jobConfig;
            this.ifNotExists = ifNotExists;
            this.dmlPlan = dmlPlan;
            this.planExecutor = planExecutor;
        }

        @Override
        public JobConfig getJobConfig() {
            return jobConfig;
        }

        @Override
        public boolean isIfNotExists() {
            return ifNotExists;
        }

        @Override
        public SelectOrSinkPlan getExecutionPlan() {
            return dmlPlan;
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
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class CacheableCreateJobPlan extends CacheableJetPlan implements CreateJobPlan {
        private final JobConfig jobConfig;
        private final boolean ifNotExists;
        private final CacheableSelectOrSinkPlan dmlPlan;
        private final JetPlanExecutor planExecutor;

        private CacheableCreateJobPlan(
                PlanCacheKey id,
                JobConfig jobConfig,
                boolean ifNotExists,
                CacheableSelectOrSinkPlan dmlPlan,
                JetPlanExecutor planExecutor
        ) {
            super(id);

            this.jobConfig = jobConfig;
            this.ifNotExists = ifNotExists;
            this.dmlPlan = dmlPlan;
            this.planExecutor = planExecutor;
        }

        @Override
        public JobConfig getJobConfig() {
            return jobConfig;
        }

        @Override
        public boolean isIfNotExists() {
            return ifNotExists;
        }

        @Override
        public SelectOrSinkPlan getExecutionPlan() {
            return dmlPlan;
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
        public boolean isPlanValid(PlanCheckContext context) {
            return dmlPlan.isPlanValid(context);
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class AlterJobPlan implements JetPlan {
        private final String jobName;
        private final AlterJobOperation operation;
        private final JetPlanExecutor planExecutor;

        private AlterJobPlan(
                String jobName,
                AlterJobOperation operation,
                JetPlanExecutor planExecutor
        ) {
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
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class DropJobPlan implements JetPlan {
        private final String jobName;
        private final boolean ifExists;
        private final String withSnapshotName;
        private final JetPlanExecutor planExecutor;

        private DropJobPlan(
                String jobName,
                boolean ifExists,
                String withSnapshotName,
                JetPlanExecutor planExecutor
        ) {
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
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class CreateSnapshotPlan implements JetPlan {
        private final String snapshotName;
        private final String jobName;
        private final JetPlanExecutor planExecutor;

        private CreateSnapshotPlan(
                String snapshotName,
                String jobName,
                JetPlanExecutor planExecutor
        ) {
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
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class DropSnapshotPlan implements JetPlan {
        private final String snapshotName;
        private final boolean ifExists;
        private final JetPlanExecutor planExecutor;

        private DropSnapshotPlan(
                String snapshotName,
                boolean ifExists,
                JetPlanExecutor planExecutor
        ) {
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
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return false;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    class ShowStatementPlan implements JetPlan {
        private final ShowStatementTarget showTarget;
        private final JetPlanExecutor planExecutor;

        private ShowStatementPlan(ShowStatementTarget showTarget, JetPlanExecutor planExecutor) {
            this.showTarget = showTarget;
            this.planExecutor = planExecutor;
        }

        ShowStatementTarget getShowTarget() {
            return showTarget;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
        }

        @Override
        public boolean producesRows() {
            return true;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this);
        }
    }

    interface SelectOrSinkPlan extends JetPlan {
        DAG getDag();

        boolean isStreaming();

        boolean isInsert();

        SqlRowMetadata getRowMetadata();
    }

    class NonCacheableSelectOrSinkPlan implements SelectOrSinkPlan {
        private final DAG dag;
        private final boolean isStreaming;
        private final boolean isInsert;
        private final SqlRowMetadata rowMetadata;
        private final JetPlanExecutor planExecutor;
        private final List<Permission> permissions;

        private NonCacheableSelectOrSinkPlan(
                DAG dag,
                boolean isStreaming,
                boolean isInsert,
                SqlRowMetadata rowMetadata,
                JetPlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            this.dag = dag;
            this.isStreaming = isStreaming;
            this.isInsert = isInsert;
            this.rowMetadata = rowMetadata;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        @Override
        public DAG getDag() {
            return dag;
        }

        @Override
        public boolean isStreaming() {
            return isStreaming;
        }

        @Override
        public boolean isInsert() {
            return isInsert;
        }

        @Override
        public SqlRowMetadata getRowMetadata() {
            return rowMetadata;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            for (Permission permission : permissions) {
                context.checkPermission(permission);
            }
        }

        @Override
        public boolean producesRows() {
            return !isInsert;
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this, queryId);
        }
    }

    class CacheableSelectOrSinkPlan extends CacheableJetPlan implements SelectOrSinkPlan {
        private final Set<PlanObjectKey> objectKeys;

        private final DAG dag;
        private final boolean isStreaming;
        private final boolean isInsert;
        private final SqlRowMetadata rowMetadata;
        private final JetPlanExecutor planExecutor;
        private final List<Permission> permissions;

        private CacheableSelectOrSinkPlan(
                PlanCacheKey id,
                Set<PlanObjectKey> objectKeys,
                DAG dag,
                boolean isStreaming,
                boolean isInsert,
                SqlRowMetadata rowMetadata,
                JetPlanExecutor planExecutor,
                List<Permission> permissions
        ) {
            super(id);

            this.objectKeys = objectKeys;

            this.dag = dag;
            this.isStreaming = isStreaming;
            this.isInsert = isInsert;
            this.rowMetadata = rowMetadata;
            this.planExecutor = planExecutor;
            this.permissions = permissions;
        }

        @Override
        public DAG getDag() {
            return dag;
        }

        @Override
        public boolean isStreaming() {
            return isStreaming;
        }

        @Override
        public boolean isInsert() {
            return isInsert;
        }

        @Override
        public SqlRowMetadata getRowMetadata() {
            return rowMetadata;
        }

        @Override
        public void checkPermissions(SqlSecurityContext context) {
            for (Permission permission : permissions) {
                context.checkPermission(permission);
            }
        }

        @Override
        public boolean producesRows() {
            return !isInsert;
        }

        @Override
        public boolean isPlanValid(PlanCheckContext context) {
            return context.isValid(objectKeys);
        }

        @Override
        public SqlResult execute(QueryId queryId) {
            return planExecutor.execute(this, queryId);
        }
    }
}
