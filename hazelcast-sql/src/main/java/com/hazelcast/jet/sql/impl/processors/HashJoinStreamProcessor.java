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

package com.hazelcast.jet.sql.impl.processors;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.JetJoinInfo;
import com.hazelcast.jet.sql.impl.ObjectArrayKey;
import com.hazelcast.jet.sql.impl.QueryResultProducerImpl;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryResultProducer;
import com.hazelcast.sql.impl.ResultIterator;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.state.QueryResultRegistry;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.Util.extendArray;
import static com.hazelcast.jet.impl.util.Util.getNodeEngine;
import static com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext.SQL_ARGUMENTS_KEY_NAME;

public class HashJoinStreamProcessor extends AbstractProcessor {
    private static final int TIMEOUT = 200000;

    private final JetJoinInfo joinInfo;
    private final int rightInputColumnCount;
    private final DAG subDag;

    private ExpressionEvalContext evalContext;
    private Multimap<ObjectArrayKey, Object[]> hashMap;
    private FlatMapper<Object[], Object[]> flatMapper;
    private JetService jet;
    private QueryResultRegistry queryResultRegistry;

    public HashJoinStreamProcessor(
            JetJoinInfo joinInfo,
            int rightInputColumnCount,
            DAG subDag
    ) {
        this.joinInfo = joinInfo;
        this.rightInputColumnCount = rightInputColumnCount;
        this.subDag = subDag;
    }

    @Override
    public void init(@Nonnull Context context) throws Exception {
        this.evalContext = SimpleExpressionEvalContext.from(context);
        this.hashMap = LinkedListMultimap.create();
        this.flatMapper = flatMapper(this::join);
        this.jet = context.hazelcastInstance().getJet();
        this.queryResultRegistry = getNodeEngine(context.hazelcastInstance())
                .getSqlService()
                .getInternalService()
                .getResultRegistry();
    }

    private Traverser<Object[]> join(Object[] leftRow) {
        ObjectArrayKey joinKeys = ObjectArrayKey.project(leftRow, joinInfo.leftEquiJoinIndices());
        Collection<Object[]> matchedRows = hashMap.get(joinKeys);
        List<Object[]> output = matchedRows.stream()
                .map(right -> ExpressionUtil.join(
                        leftRow,
                        right,
                        joinInfo.nonEquiCondition(),
                        evalContext)
                )
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (joinInfo.isLeftOuter() && output.isEmpty()) {
            return Traversers.singleton(extendArray(leftRow, rightInputColumnCount));
        }
        return Traversers.traverseIterable(output);
    }

    private ResultIterator<Row> executeDag() {
        JobConfig jobConfig = new JobConfig()
                .setArgument(SQL_ARGUMENTS_KEY_NAME, evalContext.getArguments())
                .setTimeoutMillis(TIMEOUT);

        QueryResultProducer queryResultProducer = new QueryResultProducerImpl(true);
        Job job = jet.newJob(subDag, jobConfig);
        long jobId = job.getId();
        QueryResultProducer old = queryResultRegistry.store(jobId, queryResultProducer);
        assert old == null : old;
        try {
            job.getFuture().whenComplete((r, t) -> {
                if (t != null) {
                    queryResultProducer.onError(QueryException.error(1, "The Jet SQL job failed: ", t));
                }
            });
        } catch (Throwable e) {
            queryResultRegistry.remove(jobId);
            throw e;
        }
        return queryResultProducer.iterator();
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        assert ordinal == 0;

        hashMap.clear();
        ResultIterator<Row> rightResult = executeDag();
        rightResult.forEachRemaining(row -> {
            int rightLen = row.getColumnCount();
            Object[] rightRow = rowToArray(row, rightLen);
            ObjectArrayKey joinKeys = ObjectArrayKey.project(rightRow, joinInfo.rightEquiJoinIndices());
            hashMap.put(joinKeys, rightRow);
        });
        super.process(ordinal, inbox);
    }

    private static Object[] rowToArray(Row row, int len) {
        Object[] result = new Object[len];
        for (int i = 0; i < len; i++) {
            result[i] = row.get(i);
        }
        return result;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        return flatMapper.tryProcess((Object[]) item);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static HashJoinStreamProcessorSupplier supplier(
            JetJoinInfo joinInfo,
            int rightInputColumnCount,
            DAG dag
    ) {
        return new HashJoinStreamProcessorSupplier(joinInfo, rightInputColumnCount, dag);
    }

    private static final class HashJoinStreamProcessorSupplier implements ProcessorSupplier, DataSerializable {
        private JetJoinInfo joinInfo;
        private int rightInputColumnCount;
        private DAG subDag;
        private SqlRowMetadata rightRowMetadata;

        @SuppressWarnings("unused") // for deserialization
        private HashJoinStreamProcessorSupplier() {
        }

        private HashJoinStreamProcessorSupplier(
                JetJoinInfo joinInfo,
                int rightInputColumnCount,
                DAG subDag
        ) {
            this.joinInfo = joinInfo;
            this.rightInputColumnCount = rightInputColumnCount;
            this.subDag = subDag;
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<HashJoinStreamProcessor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                processors.add(new HashJoinStreamProcessor(joinInfo, rightInputColumnCount, subDag));
            }
            return processors;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(joinInfo);
            out.writeInt(rightInputColumnCount);
            out.writeObject(subDag);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            joinInfo = in.readObject();
            rightInputColumnCount = in.readInt();
            subDag = in.readObject();
        }
    }
}
