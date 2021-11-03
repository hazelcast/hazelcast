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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.AlterJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DmlPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropJobPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropMappingPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.ExplainStatementPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapDeletePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapInsertPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapSelectPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapSinkPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.IMapUpdatePlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.SelectPlan;
import com.hazelcast.jet.sql.impl.SqlPlanImpl.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.map.impl.EntryRemovingProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.UpdateSqlResultImpl;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.state.QueryResultRegistry;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static com.hazelcast.sql.SqlColumnType.VARCHAR;
import static java.util.Collections.singletonList;

public class PlanExecutor {
    private static final String LE = System.lineSeparator();

    private final MappingCatalog catalog;
    private final HazelcastInstance hazelcastInstance;
    private final QueryResultRegistry resultRegistry;

    public PlanExecutor(
            MappingCatalog catalog,
            HazelcastInstance hazelcastInstance,
            QueryResultRegistry resultRegistry
    ) {
        this.catalog = catalog;
        this.hazelcastInstance = hazelcastInstance;
        this.resultRegistry = resultRegistry;
    }

    SqlResult execute(CreateMappingPlan plan) {
        catalog.createMapping(plan.mapping(), plan.replace(), plan.ifNotExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropMappingPlan plan) {
        catalog.removeMapping(plan.name(), plan.ifExists());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateJobPlan plan, List<Object> arguments) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = plan.getJobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, args);
        if (plan.isIfNotExists()) {
            hazelcastInstance.getJet().newJobIfAbsent(plan.getExecutionPlan().getDag(), jobConfig);
        } else {
            hazelcastInstance.getJet().newJob(plan.getExecutionPlan().getDag(), jobConfig);
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(AlterJobPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
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
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropJobPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        boolean jobTerminated = job != null && job.getStatus().isTerminal();
        if (job == null || jobTerminated) {
            if (plan.isIfExists()) {
                return UpdateSqlResultImpl.createUpdateCountResult(0);
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
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(CreateSnapshotPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }
        job.exportSnapshot(plan.getSnapshotName());
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropSnapshotPlan plan) {
        JobStateSnapshot snapshot = hazelcastInstance.getJet().getJobStateSnapshot(plan.getSnapshotName());
        if (snapshot == null) {
            if (plan.isIfExists()) {
                return UpdateSqlResultImpl.createUpdateCountResult(0);
            }
            throw QueryException.error("The snapshot doesn't exist: " + plan.getSnapshotName());
        }
        snapshot.destroy();
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(ShowStatementPlan plan) {
        Stream<String> rows;
        if (plan.getShowTarget() == ShowStatementTarget.MAPPINGS) {
            rows = catalog.getMappingNames().stream();
        } else {
            assert plan.getShowTarget() == ShowStatementTarget.JOBS;
            NodeEngine nodeEngine = getNodeEngine(hazelcastInstance);
            JetServiceBackend jetServiceBackend = nodeEngine.getService(JetServiceBackend.SERVICE_NAME);
            rows = jetServiceBackend.getJobRepository().getJobRecords().stream()
                    .map(record -> record.getConfig().getName())
                    .filter(Objects::nonNull);
        }
        SqlRowMetadata metadata = new SqlRowMetadata(singletonList(new SqlColumnMetadata("name", VARCHAR, false)));
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);

        return new SqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new StaticQueryResultProducerImpl(rows.sorted().map(name -> new HeapRow(new Object[]{name})).iterator()),
                metadata,
                false,
                serializationService
        );
    }

    SqlResult execute(ExplainStatementPlan plan) {
        Stream<String> planRows;
        SqlRowMetadata metadata = new SqlRowMetadata(
                singletonList(
                        new SqlColumnMetadata("rel", VARCHAR, false)
                )
        );
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);

        planRows = Arrays.stream(plan.getRel().explain().split(LE));
        return new SqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new StaticQueryResultProducerImpl(planRows.map(rel -> new HeapRow(new Object[]{rel})).iterator()),
                metadata,
                false,
                serializationService
        );
    }

    SqlResult execute(SelectPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setTimeoutMillis(timeout);

        QueryResultProducerImpl queryResultProducer = new QueryResultProducerImpl(!plan.isStreaming());
        AbstractJetInstance<?> jet = (AbstractJetInstance<?>) hazelcastInstance.getJet();
        long jobId = jet.newJobId();
        Object oldValue = resultRegistry.store(jobId, queryResultProducer);
        assert oldValue == null : oldValue;
        try {
            Job job = jet.newLightJob(jobId, plan.getDag(), jobConfig);
            job.getFuture().whenComplete((r, t) -> {
                if (t != null) {
                    int errorCode = findQueryExceptionCode(t);
                    String errorMessage = findQueryExceptionMessage(t);
                    queryResultProducer.onError(QueryException.error(errorCode, "The Jet SQL job failed: " + errorMessage, t));
                }
            });
        } catch (Throwable e) {
            resultRegistry.remove(jobId);
            throw e;
        }

        return new SqlResultImpl(
                queryId,
                queryResultProducer,
                plan.getRowMetadata(),
                plan.isStreaming(),
                serializationService
        );
    }

    SqlResult execute(DmlPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setTimeoutMillis(timeout);

        Job job = hazelcastInstance.getJet().newLightJob(plan.getDag(), jobConfig);
        job.join();

        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapSelectPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        InternalSerializationService serializationService = Util.getSerializationService(hazelcastInstance);
        SimpleExpressionEvalContext evalContext = new SimpleExpressionEvalContext(args, serializationService);
        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Object[]> future = hazelcastInstance.getMap(plan.mapName())
                .getAsync(key)
                .toCompletableFuture()
                .thenApply(value -> plan.rowProjectorSupplier()
                        .get(evalContext, Extractors.newBuilder(serializationService).build())
                        .project(key, value));
        Object[] row = await(future, timeout);
        return new SqlResultImpl(
                queryId,
                new StaticQueryResultProducerImpl(row),
                plan.rowMetadata(),
                false,
                serializationService
        );
    }

    SqlResult execute(IMapInsertPlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        SimpleExpressionEvalContext evalContext =
                new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        List<Entry<Object, Object>> entries = plan.entriesFn().apply(evalContext);
        if (!entries.isEmpty()) {
            assert entries.size() == 1;
            Entry<Object, Object> entry = entries.get(0);
            CompletableFuture<Object> future = ((MapProxyImpl<Object, Object>) hazelcastInstance.getMap(plan.mapName()))
                    .putIfAbsentAsync(entry.getKey(), entry.getValue())
                    .toCompletableFuture();
            Object previous = await(future, timeout);
            if (previous != null) {
                throw QueryException.error("Duplicate key");
            }
        }
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapSinkPlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        SimpleExpressionEvalContext evalContext =
                new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        Map<Object, Object> entries = plan.entriesFn().apply(evalContext);
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .putAllAsync(entries)
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapUpdatePlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        SimpleExpressionEvalContext evalContext =
                new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Long> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, plan.updaterSupplier().get(arguments))
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapDeletePlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        SimpleExpressionEvalContext evalContext =
                new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance));
        Object key = plan.keyCondition().eval(EmptyRow.INSTANCE, evalContext);
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR)
                .toCompletableFuture();
        await(future, timeout);
        return UpdateSqlResultImpl.createUpdateCountResult(0);
    }

    private List<Object> prepareArguments(QueryParameterMetadata parameterMetadata, List<Object> arguments) {
        assert arguments != null;

        int parameterCount = parameterMetadata.getParameterCount();
        if (parameterCount != arguments.size()) {
            throw QueryException.error(
                    SqlErrorCode.DATA_EXCEPTION,
                    "Unexpected parameter count: expected " + parameterCount + ", got " + arguments.size()
            );
        }

        for (int i = 0; i < arguments.size(); ++i) {
            Object value = arguments.get(i);

            ParameterConverter parameterConverter = parameterMetadata.getParameterConverter(i);

            Object newValue = parameterConverter.convert(value);

            if (newValue != value) {
                arguments.set(i, newValue);
            }
        }

        return arguments;
    }

    private static int findQueryExceptionCode(Throwable t) {
        while (t != null) {
            if (t instanceof QueryException) {
                return ((QueryException) t).getCode();
            }
            t = t.getCause();
        }
        return SqlErrorCode.GENERIC;
    }

    private static String findQueryExceptionMessage(Throwable t) {
        while (t != null) {
            if (t.getMessage() != null) {
                return t.getMessage();
            }
            t = t.getCause();
        }
        return "";
    }

    private <T> T await(CompletableFuture<T> future, long timeout) {
        try {
            return timeout > 0 ? future.get(timeout, TimeUnit.MILLISECONDS) : future.get();
        } catch (TimeoutException e) {
            future.cancel(true);
            throw QueryException.error("Timeout occurred while executing statement");
        } catch (InterruptedException | ExecutionException e) {
            throw QueryException.error(e.getMessage(), e);
        }
    }
}
