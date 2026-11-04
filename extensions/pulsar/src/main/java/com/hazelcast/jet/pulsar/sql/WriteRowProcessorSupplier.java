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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pulsar.impl.PulsarSinkConfig;
import com.hazelcast.jet.pulsar.impl.WritePulsarP;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
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
final class WriteRowProcessorSupplier implements ProcessorSupplier, DataSerializable {
    @Serial
    private static final long serialVersionUID = 1L;

    private KvProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;

    private transient PulsarClient client;

    private PulsarSinkConfig<Object, Object> pulsarConfig;

    @SuppressWarnings("unused")
    private WriteRowProcessorSupplier() {
    }

    WriteRowProcessorSupplier(
        PulsarSinkConfig pulsarConfig,
        QueryPath[] paths,
        QueryDataType[] types,
        UpsertTargetDescriptor keyDescriptor,
        UpsertTargetDescriptor valueDescriptor
    ) {
        this.pulsarConfig = pulsarConfig;
        this.projectorSupplier = KvProjector.supplier(
            paths,
            types,
            keyDescriptor,
            valueDescriptor,
            false
        );
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = ExpressionEvalContext.from(context);
        client = pulsarConfig.getTargetConnectionSupplier(context).get();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            KvProjector projector = projectorSupplier.get(evalContext);

            var confCopy = new PulsarSinkConfig(pulsarConfig);
            confCopy.setExtractKeyAndValueFn(row -> projector.project((JetSqlRow) row));

            Processor processor = new WritePulsarP<>(confCopy, client);
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void close(@Nullable Throwable error) throws Exception {
        if (client != null) {
            client.shutdown();
        }
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(pulsarConfig);
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        pulsarConfig = in.readObject();
        projectorSupplier = in.readObject();
    }
}
