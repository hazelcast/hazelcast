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
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.JetPlan.AlterJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DmlPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.IMapDeletePlan;
import com.hazelcast.jet.sql.impl.JetPlan.IMapInsertPlan;
import com.hazelcast.jet.sql.impl.JetPlan.IMapSinkPlan;
import com.hazelcast.jet.sql.impl.JetPlan.IMapUpdatePlan;
import com.hazelcast.jet.sql.impl.JetPlan.SelectPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.map.impl.EntryRemovingProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlColumnType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.ParameterConverter;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryParameterMetadata;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.SqlResultImpl;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.HeapRow;

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
import static java.util.Collections.singletonList;

public class JetPlanExecutor {

    private final MappingCatalog catalog;
    private final HazelcastInstance hazelcastInstance;
    private final Map<Long, JetQueryResultProducer> resultConsumerRegistry;

    public JetPlanExecutor(
            MappingCatalog catalog,
            HazelcastInstance hazelcastInstance,
            Map<Long, JetQueryResultProducer> resultConsumerRegistry
    ) {
        this.catalog = catalog;
        this.hazelcastInstance = hazelcastInstance;
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

    SqlResult execute(CreateJobPlan plan, List<Object> arguments) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = plan.getJobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, args);
        if (plan.isIfNotExists()) {
            hazelcastInstance.getJet().newJobIfAbsent(plan.getExecutionPlan().getDag(), jobConfig);
        } else {
            hazelcastInstance.getJet().newJob(plan.getExecutionPlan().getDag(), jobConfig);
        }
        return SqlResultImpl.createUpdateCountResult(0);
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
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropJobPlan plan) {
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
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
        Job job = hazelcastInstance.getJet().getJob(plan.getJobName());
        if (job == null) {
            throw QueryException.error("The job '" + plan.getJobName() + "' doesn't exist");
        }
        job.exportSnapshot(plan.getSnapshotName());
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(DropSnapshotPlan plan) {
        JobStateSnapshot snapshot = hazelcastInstance.getJet().getJobStateSnapshot(plan.getSnapshotName());
        if (snapshot == null) {
            if (plan.isIfExists()) {
                return SqlResultImpl.createUpdateCountResult(0);
            }
            throw QueryException.error("The snapshot doesn't exist: " + plan.getSnapshotName());
        }
        snapshot.destroy();
        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(ShowStatementPlan plan) {
        SqlRowMetadata metadata = new SqlRowMetadata(
                singletonList(new SqlColumnMetadata("name", SqlColumnType.VARCHAR, false)));
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

        return new JetSqlResultImpl(
                QueryId.create(hazelcastInstance.getLocalEndpoint().getUuid()),
                new JetStaticQueryResultProducer(rows.sorted().map(name -> new HeapRow(new Object[]{name})).iterator()),
                metadata,
                false);
    }

    SqlResult execute(SelectPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setTimeoutMillis(timeout);

        JetQueryResultProducer queryResultProducer = new JetQueryResultProducer(getSerializationService());
        AbstractJetInstance<?> jet = (AbstractJetInstance<?>) hazelcastInstance.getJet();
        Long jobId = jet.newJobId();
        Object oldValue = resultConsumerRegistry.put(jobId, queryResultProducer);
        assert oldValue == null : oldValue;
        try {
            Job job = jet.newLightJob(jobId, plan.getDag(), jobConfig);
            job.getFuture().whenComplete((r, t) -> {
                if (t != null) {
                    int errorCode = findQueryExceptionCode(t);
                    queryResultProducer.onError(
                            QueryException.error(errorCode, "The Jet SQL job failed: " + t.getMessage(), t));
                }
            });
        } catch (Throwable e) {
            resultConsumerRegistry.remove(jobId);
            throw e;
        }

        return new JetSqlResultImpl(queryId, queryResultProducer, plan.getRowMetadata(), plan.isStreaming());
    }

    SqlResult execute(DmlPlan plan, QueryId queryId, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, args)
                .setTimeoutMillis(timeout);

        Job job = hazelcastInstance.getJet().newLightJob(plan.getDag(), jobConfig);
        job.join();

        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(IMapInsertPlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        List<Entry<Object, Object>> entries = plan.entriesFn()
                .apply(new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance)));
        if (entries.isEmpty()) {
            return SqlResultImpl.createUpdateCountResult(0);
        } else {
            assert entries.size() == 1;
            Entry<Object, Object> entry = entries.get(0);
            CompletableFuture<Object> future = ((MapProxyImpl<Object, Object>) hazelcastInstance.getMap(plan.mapName()))
                    .putIfAbsentAsync(entry.getKey(), entry.getValue())
                    .toCompletableFuture();
            try {
                Object previous;
                if (timeout > 0) {
                    previous = future.get(timeout, TimeUnit.MILLISECONDS);
                } else {
                    previous = future.get();
                }
                if (previous != null) {
                    throw QueryException.error("Duplicate key");
                }
                return SqlResultImpl.createUpdateCountResult(0);
            } catch (TimeoutException e) {
                future.cancel(true);
                throw QueryException.error("Timeout occurred while inserting entry");
            } catch (InterruptedException | ExecutionException e) {
                throw QueryException.error(e.getMessage(), e);
            }
        }
    }

    SqlResult execute(IMapSinkPlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        Map<Object, Object> entries = plan.entriesFn()
                .apply(new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance)));
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .putAllAsync(entries)
                .toCompletableFuture();
        try {
            if (timeout > 0) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
            return SqlResultImpl.createUpdateCountResult(0);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw QueryException.error("Timeout occurred while inserting entries");
        } catch (InterruptedException | ExecutionException e) {
            throw QueryException.error(e.getMessage(), e);
        }
    }

    SqlResult execute(IMapUpdatePlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        Object key = plan.keyCondition()
                .eval(
                        EmptyRow.INSTANCE,
                        new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance))
                );
        CompletableFuture<Long> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, plan.updaterSupplier().get(arguments))
                .toCompletableFuture();
        try {
            if (timeout > 0) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
            return SqlResultImpl.createUpdateCountResult(0);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw QueryException.error("Timeout occurred while updating an entry");
        } catch (InterruptedException | ExecutionException e) {
            throw QueryException.error(e.getMessage(), e);
        }
    }

    SqlResult execute(IMapDeletePlan plan, List<Object> arguments, long timeout) {
        List<Object> args = prepareArguments(plan.parameterMetadata(), arguments);
        Object key = plan.keyCondition()
                .eval(
                        EmptyRow.INSTANCE,
                        new SimpleExpressionEvalContext(args, Util.getSerializationService(hazelcastInstance))
                );
        CompletableFuture<Void> future = hazelcastInstance.getMap(plan.mapName())
                .submitToKey(key, EntryRemovingProcessor.ENTRY_REMOVING_PROCESSOR)
                .toCompletableFuture();
        try {
            if (timeout > 0) {
                future.get(timeout, TimeUnit.MILLISECONDS);
            } else {
                future.get();
            }
            return SqlResultImpl.createUpdateCountResult(0);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw QueryException.error("Timeout occurred while deleting an entry");
        } catch (InterruptedException | ExecutionException e) {
            throw QueryException.error(e.getMessage(), e);
        }
    }

    private InternalSerializationService getSerializationService() {
        return ((HazelcastInstanceImpl) hazelcastInstance).getSerializationService();
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
}
