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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.impl.util.Util.extendArray;
import static java.util.concurrent.CompletableFuture.completedFuture;

@SuppressFBWarnings(
        value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
        justification = "the class is never java-serialized"
)
final class JoinByPrimitiveKeyProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private static final int MAX_CONCURRENT_OPS = 8;

    private boolean inner;
    private int leftEquiJoinIndex;
    private Expression<Boolean> condition;
    private String mapName;
    private KvRowProjector.Supplier rightRowProjectorSupplier;

    private transient IMap<Object, Object> map;
    private transient ExpressionEvalContext evalContext;
    private transient Extractors extractors;

    @SuppressWarnings("unused")
    private JoinByPrimitiveKeyProcessorSupplier() {
    }

    JoinByPrimitiveKeyProcessorSupplier(
            boolean inner,
            int leftEquiJoinIndex,
            Expression<Boolean> condition,
            String mapName,
            KvRowProjector.Supplier rightRowProjectorSupplier
    ) {
        this.inner = inner;
        this.leftEquiJoinIndex = leftEquiJoinIndex;
        this.condition = condition;
        this.mapName = mapName;
        this.rightRowProjectorSupplier = rightRowProjectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = context.hazelcastInstance().getMap(mapName);
        evalContext = SimpleExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            KvRowProjector projector = rightRowProjectorSupplier.get(evalContext, extractors);
            TransientReference<IMap<Object, Object>> context = new TransientReference<>(map);
            Processor processor = new AsyncTransformUsingServiceOrderedP<>(
                    ServiceFactories.nonSharedService(ctx -> context),
                    null,
                    MAX_CONCURRENT_OPS,
                    (TransientReference<IMap<Object, Object>> ctx, Object[] left) -> {
                        Object key = left[leftEquiJoinIndex];
                        if (key == null) {
                            return inner ? null : completedFuture(null);
                        }
                        return ctx.ref.getAsync(key).toCompletableFuture();
                    },
                    (left, value) -> {
                        Object[] joined = join(left, left[leftEquiJoinIndex], value, projector, condition, evalContext);
                        return joined != null ? singleton(joined)
                                : inner ? null
                                : singleton(extendArray(left, projector.getColumnCount()));
                    }
            );
            processors.add(processor);
        }
        return processors;
    }

    private static Object[] join(
            Object[] left,
            Object key,
            Object value,
            KvRowProjector rightRowProjector,
            Expression<Boolean> condition,
            ExpressionEvalContext evalContext
    ) {
        if (value == null) {
            return null;
        }

        Object[] right = rightRowProjector.project(entry(key, value));
        if (right == null) {
            return null;
        }

        return ExpressionUtil.join(left, right, condition, evalContext);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(inner);
        out.writeInt(leftEquiJoinIndex);
        out.writeObject(condition);
        out.writeObject(mapName);
        out.writeObject(rightRowProjectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        inner = in.readBoolean();
        leftEquiJoinIndex = in.readInt();
        condition = in.readObject();
        mapName = in.readObject();
        rightRowProjectorSupplier = in.readObject();
    }

    /**
     * A reference to an object, which is serializable, but the reference is
     * transient. Used to workaround the need for ServiceContext to be
     * serializable, but never actually serialized.
     */
    @SuppressFBWarnings(
            value = {"SE_TRANSIENT_FIELD_NOT_RESTORED"},
            justification = "the class is never serialized"
    )
    private static final class TransientReference<T> implements Serializable {

        private final transient T ref;

        private TransientReference(T ref) {
            this.ref = ref;
        }
    }
}
