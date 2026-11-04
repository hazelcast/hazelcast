/*
 * Copyright 2026 Hazelcast Inc.
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

package com.hazelcast.jet.pulsar.sql;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pulsar.impl.PulsarSourceConfig;
import com.hazelcast.jet.pulsar.impl.StreamPulsarViaReaderP;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.pulsar.client.api.PulsarClient;
import org.jspecify.annotations.Nullable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serial;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@SuppressWarnings({"rawtypes", "unchecked"})
final class RowProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    private KvRowProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;
    private transient EventTimePolicy<JetSqlRow> eventTimePolicy;
    private transient Extractors extractors;

    private PulsarSourceConfig<Object, Object> pulsarConfig;
    private PulsarClient pulsarClient;

    @SuppressWarnings("unused")
    private RowProjectorProcessorSupplier() {
    }

    RowProjectorProcessorSupplier(
            PulsarSourceConfig pulsarConfig,
            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        this.pulsarConfig = pulsarConfig;
        this.eventTimePolicyProvider = eventTimePolicyProvider;
        this.projectorSupplier = KvRowProjector.supplier(
                paths,
                types,
                keyDescriptor,
                valueDescriptor,
                predicate,
                projection
        );
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
        eventTimePolicy = eventTimePolicyProvider == null
                ? EventTimePolicy.noEventTime()
                : eventTimePolicyProvider.apply(evalContext);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
        pulsarClient = pulsarConfig.getTargetConnectionSupplier(context).get();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            KvRowProjector projector = projectorSupplier.get(evalContext, extractors);
            pulsarConfig.setProjectionFn(msg -> projector.project(msg.getKey(), msg.getValue()));

            var rawTypeConf = new PulsarSourceConfig(pulsarConfig);
            Processor processor = new StreamPulsarViaReaderP<>(rawTypeConf, eventTimePolicy, pulsarClient);
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        if (pulsarClient != null) {
            pulsarClient.shutdown();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(pulsarConfig);
        out.writeObject(eventTimePolicyProvider);
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pulsarConfig = in.readObject();
        eventTimePolicyProvider = in.readObject();
        projectorSupplier = in.readObject();
    }
}
