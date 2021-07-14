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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class RowProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private KvRowProjector.Supplier projectorSupplier;

    private transient ExpressionEvalContext evalContext;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private RowProjectorProcessorSupplier() {
    }

    public RowProjectorProcessorSupplier(KvRowProjector.Supplier projectorSupplier) {
        this.projectorSupplier = projectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        evalContext = SimpleExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ResettableSingletonTraverser<Object[]> traverser = new ResettableSingletonTraverser<>();
            KvRowProjector projector = projectorSupplier.get(evalContext, extractors);
            Processor processor = new TransformP<Map.Entry<Object, Object>, Object[]>(entry -> {
                traverser.accept(projector.project(entry.getKey(), entry.getValue()));
                return traverser;
            });
            processors.add(processor);
        }
        return processors;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        projectorSupplier = in.readObject();
    }

    public static ProcessorSupplier rowProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        return new RowProjectorProcessorSupplier(
                KvRowProjector.supplier(paths, types, keyDescriptor, valueDescriptor, predicate, projection));
    }
}
