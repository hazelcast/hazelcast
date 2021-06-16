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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

final class UpdateProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private static final int MAX_CONCURRENT_OPS = 8;

    private String mapName;
    private KvRowProjector.Supplier rowProjectorSupplier;
    private List<Expression<?>> projections;
    private KvProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;

    @SuppressWarnings("unused")
    private UpdateProcessorSupplier() {
    }

    UpdateProcessorSupplier(
            String mapName,
            KvRowProjector.Supplier rowProjectorSupplier,
            List<Expression<?>> projections,
            KvProjector.Supplier projectorSupplier
    ) {
        this.mapName = mapName;
        this.rowProjectorSupplier = rowProjectorSupplier;
        this.projections = projections;
        this.projectorSupplier = projectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = SimpleExpressionEvalContext.from(context);
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String localMapName = mapName;
            Processor processor = new AsyncTransformUsingServiceOrderedP<>(
                    ServiceFactories.nonSharedService(ctx -> ctx.hazelcastInstance().getMap(localMapName)),
                    null,
                    MAX_CONCURRENT_OPS,
                    (IMap<Object, Object> ctx, Object[] row) -> update(row, ctx),
                    (row, numberOfUpdatedEntries) -> Traversers.singleton(numberOfUpdatedEntries)
            );
            processors.add(processor);
        }
        return processors;
    }

    private CompletableFuture<?> update(Object[] row, IMap<Object, Object> map) {
        assert row.length == 1;
        Object key = row[0];
        return map.submitToKey(
                key,
                new ValueUpdater(rowProjectorSupplier, projections, projectorSupplier, evalContext.getArguments())
        ).toCompletableFuture();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeObject(rowProjectorSupplier);
        out.writeObject(projections);
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        rowProjectorSupplier = in.readObject();
        projections = in.readObject();
        projectorSupplier = in.readObject();
    }

    private static final class ValueUpdater
            implements EntryProcessor<Object, Object, Object>, SerializationServiceAware, DataSerializable {

        private KvRowProjector.Supplier rowProjector;
        private List<Expression<?>> projections;
        private KvProjector.Supplier projector;
        private List<Object> arguments;

        private transient ExpressionEvalContext evalContext;
        private transient InternalSerializationService serializationService;
        private transient Extractors extractors;

        private transient Function<Object[], Object[]> projectionFn;

        @SuppressWarnings("unused")
        private ValueUpdater() {
        }

        private ValueUpdater(
                KvRowProjector.Supplier rowProjector,
                List<Expression<?>> projections,
                KvProjector.Supplier projector,
                List<Object> arguments
        ) {
            this.rowProjector = rowProjector;
            this.projections = projections;
            this.projector = projector;
            this.arguments = arguments;
        }

        @Override
        public Object process(Map.Entry<Object, Object> originalEntry) {
            Object[] row = rowProjector.get(evalContext, extractors).project(originalEntry.getKey(), originalEntry.getValue());
            Object[] projected = projectionFn.apply(row);
            Object value = projector.get(serializationService).projectValue(projected);
            originalEntry.setValue(value);
            return 1;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.evalContext = new SimpleExpressionEvalContext(arguments, (InternalSerializationService) serializationService);
            this.serializationService = (InternalSerializationService) serializationService;
            this.extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
            this.projectionFn = ExpressionUtil.projectionFn(projections, evalContext);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(rowProjector);
            out.writeObject(projections);
            out.writeObject(projector);
            out.writeObject(arguments);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            rowProjector = in.readObject();
            projections = in.readObject();
            projector = in.readObject();
            arguments = in.readObject();
        }
    }
}
