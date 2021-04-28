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

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.sql.impl.JetPlan.AlterJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.CreateSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DeletePlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropJobPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropMappingPlan;
import com.hazelcast.jet.sql.impl.JetPlan.DropSnapshotPlan;
import com.hazelcast.jet.sql.impl.JetPlan.SelectPlan;
import com.hazelcast.jet.sql.impl.JetPlan.SinkPlan;
import com.hazelcast.jet.sql.impl.JetPlan.ShowStatementPlan;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.jet.sql.impl.parse.SqlShowStatement.ShowStatementTarget;
import com.hazelcast.jet.sql.impl.schema.MappingCatalog;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
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
import com.hazelcast.sql.impl.expression.ColumnExpression;
import com.hazelcast.sql.impl.expression.ConstantExpression;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.expression.predicate.AndPredicate;
import com.hazelcast.sql.impl.expression.predicate.ComparisonMode;
import com.hazelcast.sql.impl.expression.predicate.ComparisonPredicate;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

class JetPlanExecutor {

    private final MappingCatalog catalog;
    private final AbstractJetInstance jetInstance;
    private final Map<Long, JetQueryResultProducer> resultConsumerRegistry;

    JetPlanExecutor(
            MappingCatalog catalog,
            AbstractJetInstance jetInstance,
            Map<Long, JetQueryResultProducer> resultConsumerRegistry
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

    SqlResult execute(CreateJobPlan plan, List<Object> arguments) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = plan.getJobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, args);

        if (plan.isIfNotExists()) {
            jetInstance.newJobIfAbsent(plan.getExecutionPlan().getDag(), jobConfig);
        } else {
            jetInstance.newJob(plan.getExecutionPlan().getDag(), jobConfig);
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
            JetService jetService = ((HazelcastInstanceImpl) jetInstance.getHazelcastInstance()).node.nodeEngine
                    .getService(JetService.SERVICE_NAME);
            rows = jetService.getJobRepository().getJobRecords().stream()
                    .map(record -> record.getConfig().getName())
                    .filter(Objects::nonNull);
        }

        return new JetSqlResultImpl(
                QueryId.create(jetInstance.getHazelcastInstance().getLocalEndpoint().getUuid()),
                new JetStaticQueryResultProducer(rows.sorted().map(name -> new HeapRow(new Object[]{name})).iterator()),
                metadata,
                false);
    }

    SqlResult execute(SinkPlan plan, QueryId queryId, List<Object> arguments) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, args);

        Job job = jetInstance.newJob(plan.getDag(), jobConfig);
        job.join();

        return SqlResultImpl.createUpdateCountResult(0);
    }

    SqlResult execute(SelectPlan plan, QueryId queryId, List<Object> arguments) {
        List<Object> args = prepareArguments(plan.getParameterMetadata(), arguments);
        JobConfig jobConfig = new JobConfig().setArgument(SQL_ARGUMENTS_KEY_NAME, args);

        JetQueryResultProducer queryResultProducer = new JetQueryResultProducer();
        Long jobId = jetInstance.newJobId();
        Object oldValue = resultConsumerRegistry.put(jobId, queryResultProducer);
        assert oldValue == null : oldValue;
        try {
            Job job = jetInstance.newJob(jobId, plan.getDag(), jobConfig);
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

    @SuppressWarnings("checkstyle:nestedifdepth")
    public SqlResult execute(DeletePlan plan, QueryId queryId) {
        if (plan.getEarlyExit()) {
            return SqlResultImpl.createUpdateCountResult(0);
        }
        AbstractMapTable table = plan.getTable().getTarget();
        IMap<Object, Object> map = jetInstance.getMap(table.getSqlName());
        List<TableField> fields = table.getFields();
        QueryPath[] paths = fields.stream().map(field -> ((MapTableField) field).getPath()).toArray(QueryPath[]::new);
        QueryDataType[] types = fields.stream().map(TableField::getType).toArray(QueryDataType[]::new);
        QueryTargetDescriptor keyDescriptor = table.getKeyDescriptor();
        QueryTargetDescriptor valueDescriptor = table.getValueDescriptor();

        Expression<Boolean> filter = plan.filter();
        if (filter instanceof ComparisonPredicate) {
            Object key = extractKey((ComparisonPredicate) filter);
            if (key == null) {
                throw QueryException.error(SqlErrorCode.GENERIC, "DELETE query has to contain __key = <const value> predicate");
            }
            Object value = map.remove(key);
            return SqlResultImpl.createUpdateCountResult(value != null ? 1 : 0);
        } else if (filter instanceof AndPredicate) {
            Object key = null;
            for (Expression<?> expr : ((AndPredicate) filter).getOperands()) {
                if (expr instanceof ComparisonPredicate) {
                    Object key0 = extractKey((ComparisonPredicate) expr);
                    if (key0 != null) {
                        if (key != null) {
                            return SqlResultImpl.createUpdateCountResult(0);
                        }
                        key = key0;
                    }
                }
            }
            if (key == null) {
                throw QueryException.error(SqlErrorCode.GENERIC, "DELETE query has to contain __key = <const value> predicate");
            }
            List<Expression<?>> projection = plan.getProjection();
            KvRowProjector.Supplier supplier = KvRowProjector.supplier(
                    paths, types, keyDescriptor, valueDescriptor, null, projection);
            boolean removed = map.executeOnKey(key, new DeleteBySingleKey(supplier, filter));
            return SqlResultImpl.createUpdateCountResult(removed ? 1 : 0);
        } else {
            throw QueryException.error(SqlErrorCode.GENERIC, "Complex DELETE queries unsupported");
        }
    }

    private static class DeleteBySingleKey
            implements EntryProcessor<Object, Object, Boolean>, DataSerializable, SerializationServiceAware {
        private KvRowProjector.Supplier supplier;
        private Expression<Boolean> filter;
        private ExpressionEvalContext evalContext;
        private Extractors extractors;

        DeleteBySingleKey(KvRowProjector.Supplier supplier, Expression<Boolean> filter) {
            this.supplier = supplier;
            this.filter = filter;
        }

        @Override
        public Boolean process(Map.Entry<Object, Object> entry) {
            KvRowProjector projector = supplier.get(evalContext, extractors);
            Boolean eval = filter.eval(new HeapRow(projector.project(entry)), evalContext);
            boolean result = eval != null && eval;
            if (result) {
                entry.setValue(null);
            }
            return result;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.evalContext =
                    new SimpleExpressionEvalContext(emptyList(), (InternalSerializationService) serializationService);
            this.extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(supplier);
            out.writeObject(filter);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            supplier = in.readObject();
            filter = in.readObject();
        }
    }

    private Object extractKey(ComparisonPredicate predicate) {
        Object key = null;
        ComparisonMode mode = predicate.getMode();
        if (mode != ComparisonMode.EQUALS) {
            throw QueryException.error(SqlErrorCode.GENERIC, mode + " predicate is not supported for DELETE queries");
        }
        Expression<?> operand1 = predicate.getOperand1();
        Expression<?> operand2 = predicate.getOperand2();
        if (isKeyColumn(operand1)) {
            if (!(operand2 instanceof ConstantExpression)) {
                throw QueryException.error(SqlErrorCode.GENERIC, "DELETE query has to contain __key = <const value> predicate");
            }
            key = operand2.eval(null, null);
        } else if (isKeyColumn(operand2)) {
            if (!(operand1 instanceof ConstantExpression)) {
                throw QueryException.error(SqlErrorCode.GENERIC, "DELETE query has to contain __key = <const value> predicate");
            }
            key = operand1.eval(null, null);
        }
        return key;
    }

    private boolean isKeyColumn(Expression<?> expression) {
        return expression instanceof ColumnExpression && ((ColumnExpression<?>) expression).getIndex() == 0;
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
