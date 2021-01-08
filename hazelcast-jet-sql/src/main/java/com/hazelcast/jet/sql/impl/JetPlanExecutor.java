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

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.sql.impl.JetPlan.AlterJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.SelectOrSinkPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.row.HeapRow;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;

import static java.util.Collections.singletonList;

class JetPlanExecutor {

    private final MappingCatalog catalog;
    private final JetInstance jetInstance;
    private final Map<String, JetQueryResultProducer> resultConsumerRegistry;

    JetPlanExecutor(
            MappingCatalog catalog,
            JetInstance jetInstance,
            Map<String, JetQueryResultProducer> resultConsumerRegistry
    ) {
        this.catalog = catalog;
        this.jetInstance = jetInstance;
        this.resultConsumerRegistry = resultConsumerRegistry;
    }

    SqlResult execute(CreateMappingPlan plan) {
        catalog.createMapping(plan.mapping(), plan.replace(), plan.ifNotExists());
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropMappingPlan plan) {
        catalog.removeMapping(plan.name(), plan.ifExists());
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateJobPlan plan) {
        if (plan.isIfNotExists()) {
            jetInstance.newJobIfAbsent(plan.getExecutionPlan().getDag(), plan.getJobConfig());
        } else {
            jetInstance.newJob(plan.getExecutionPlan().getDag(), plan.getJobConfig());
        }
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(AlterJobPlan plan) {
        Job job = jetInstance.getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }
        switch (plan.getOperation()) {
            case SUSPEND:
                job.suspend();
                break;

            case RESUME:
                job.resume();
                break;

            case RESTART:
                job.restart();
                break;

            default:
        }
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropJobPlan plan) {
        Job job = jetInstance.getJob(plan.getJobName());
        boolean jobTerminated = job != null && job.getStatus().isTerminal();
        if (job == null || jobTerminated) {
            if (plan.isIfExists()) {
                return SqlResultImpl.createUpdateCountResult(0);
            }
            if (jobTerminated) {
                throw QueryException.error("Job already terminated: " + plan.getJobName());
            } else {
                throw QueryException.error("Job doesn't exist: " + plan.getJobName());
            }
        }
        if (plan.getWithSnapshotName() != null) {
            job.cancelAndExportSnapshot(plan.getWithSnapshotName());
        } else {
            job.cancel();
        }
        try {
            job.join();
        } catch (CancellationException ignored) {
        }
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateSnapshotPlan plan) {
        Job job = jetInstance.getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }
        job.exportSnapshot(plan.getSnapshotName());
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropSnapshotPlan plan) {
        JobStateSnapshot snapshot = jetInstance.getJobStateSnapshot(plan.getSnapshotName());
        if (snapshot == null) {
            if (plan.isIfExists()) {
                return SqlResultImpl.createUpdateCountResult(0);
            }
            throw QueryException.error("The snapshot doesnt exist: " + plan.getSnapshotName());
        }
        snapshot.destroy();
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(SelectOrSinkPlan plan) {
        if (plan.isInsert()) {
            if (plan.isStreaming()) {
                // TODO [viliam] add test for this situation
                throw QueryException.error("Cannot execute a streaming DML statement without a CREATE JOB command");
            }

            Job job = jetInstance.newJob(plan.getDag());
            job.join();

            return SqlResultImpl.createUpdateCountResult(0);
        } else {
            JetQueryResultProducer queryResultProducer = new JetQueryResultProducer();
            String queryIdStr = plan.getQueryId().toString();
            Object oldValue = resultConsumerRegistry.put(queryIdStr, queryResultProducer);
            assert oldValue == null : oldValue;

            try {
                Job job = jetInstance.newJob(plan.getDag());
                job.getFuture().whenComplete((r, t) -> {
                    if (t != null) {
                        queryResultProducer.onError(QueryException.error(t.toString()));
                    }
                });
            } catch (Throwable e) {
                resultConsumerRegistry.remove(queryIdStr);
                throw e;
            }

            return new JetSqlResultImpl(plan.getQueryId(), queryResultProducer, plan.getRowMetadata());
        }
    }

    public SqlResult execute(ShowStatementPlan plan) {
        SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata("name", SqlColumnType.VARCHAR)));
        Stream<String> rows;
        if (plan.getShowTarget() == ShowStatementTarget.MAPPINGS) {
            rows = catalog.getMappingNames().stream();
        } else {
            assert plan.getShowTarget() == ShowStatementTarget.JOBS;
            JetService jetService = ((HazelcastInstanceImpl) jetInstance.getHazelcastInstance()).node.nodeEngine
                    .getService(JetService.SERVICE_NAME);
            rows = jetService.getJobRepository().getJobRecords().stream()
                             .map(record -> record.getConfig().getName())
                             .filter(Objects::nonNull);
        }

        return new JetSqlResultImpl(
                QueryId.create(jetInstance.getHazelcastInstance().getLocalEndpoint().getUuid()),
                new JetStaticQueryResultProducer(rows.sorted().map(name -> new HeapRow(new Object[]{name})).iterator()),
                metadata
        );
    }
}
