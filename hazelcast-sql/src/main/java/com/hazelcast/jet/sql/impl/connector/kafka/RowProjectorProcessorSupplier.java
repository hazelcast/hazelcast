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

package com.hazelcast.jet.sql.impl.connector.kafka;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.kafka.impl.StreamKafkaP;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class RowProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private Properties properties;
    private String topic;
    private FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider;
    private KvRowProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;
    private transient EventTimePolicy<JetSqlRow> eventTimePolicy;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private RowProjectorProcessorSupplier() {
    }

    RowProjectorProcessorSupplier(
            Properties properties,
            String topic,
            FunctionEx<ExpressionEvalContext, EventTimePolicy<JetSqlRow>> eventTimePolicyProvider,
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        this.properties = properties;
        this.topic = topic;
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
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            KvRowProjector projector = projectorSupplier.get(evalContext, extractors);
            Processor processor = new StreamKafkaP<>(
                    properties,
                    singletonList(topic),
                    record -> projector.project(record.key(), record.value()),
                    eventTimePolicy
            );
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(properties);
        out.writeString(topic);
        out.writeObject(eventTimePolicyProvider);
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        properties = in.readObject();
        topic = in.readString();
        eventTimePolicyProvider = in.readObject();
        projectorSupplier = in.readObject();
    }
}
