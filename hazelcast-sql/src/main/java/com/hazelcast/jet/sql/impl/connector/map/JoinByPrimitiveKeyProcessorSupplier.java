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
import com.hazelcast.jet.impl.processor.AsyncTransformUsingServiceOrderedP;
import com.hazelcast.jet.pipeline.ServiceFactories;
import com.hazelcast.jet.sql.impl.ExpressionUtil;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.map.IMap;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.security.impl.function.SecuredFunctions;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_READ;
import static java.util.Collections.singletonList;
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
        evalContext = ExpressionEvalContext.from(context);
        extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        List<Processor> processors = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String mapName = this.mapName;
            KvRowProjector projector = rightRowProjectorSupplier.get(evalContext, extractors);
            Processor processor = new AsyncTransformUsingServiceOrderedP<>(
                    ServiceFactories.nonSharedService(SecuredFunctions.iMapFn(mapName)),
                    null,
                    MAX_CONCURRENT_OPS,
                    (IMap<Object, Object> map, JetSqlRow left) -> {
                        Object key = left.get(leftEquiJoinIndex);
                        if (key == null) {
                            return inner ? null : completedFuture(null);
                        }
                        return map.getAsync(key).toCompletableFuture();
                    },
                    (left, value) -> {
                        JetSqlRow joined = join(left, left.get(leftEquiJoinIndex), value,
                                projector, condition, evalContext);
                        return joined != null ? singleton(joined)
                                : inner ? null
                                : singleton(left.extendedRow(projector.getColumnCount()));
                    }
            );
            processors.add(processor);
        }
        return processors;
    }

    private static JetSqlRow join(
            JetSqlRow left,
            Object key,
            Object value,
            KvRowProjector rightRowProjector,
            Expression<Boolean> condition,
            ExpressionEvalContext evalContext
    ) {
        if (value == null) {
            return null;
        }

        JetSqlRow right = rightRowProjector.project(key, value);
        if (right == null) {
            return null;
        }

        return ExpressionUtil.join(left, right, condition, evalContext);
    }

    @Override
    public List<Permission> permissions() {
        return singletonList(new MapPermission(mapName, ACTION_CREATE, ACTION_READ));
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
}
