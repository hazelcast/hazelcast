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

import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.sql.impl.ExpressionUtil.evaluate;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;

class Projector {

    private final QueryDataType[] types;

    private final UpsertTarget target;

    private final List<Expression<?>> projection;
    private final ExpressionEvalContext evalContext;

    private final UpsertInjector[] injectors;

    Projector(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTarget target,
            List<Expression<?>> projection,
            ExpressionEvalContext evalContext
    ) {
        checkTrue(types.length == projection.size(), "paths.length != projection.length");
        this.types = types;

        this.target = target;

        this.projection = projection;
        this.evalContext = evalContext;

        this.injectors = createInjectors(paths, types, target);
    }

    private static UpsertInjector[] createInjectors(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTarget target
    ) {
        UpsertInjector[] injectors = new UpsertInjector[paths.length];
        for (int i = 0; i < paths.length; i++) {
            injectors[i] = target.createInjector(paths[i].getPath(), types[i]);
        }
        return injectors;
    }

    Object project(JetSqlRow values) {
        Row row = values.getRow();
        target.init();
        for (int i = 0; i < injectors.length; i++) {
            Object projected = evaluate(projection.get(i), row, evalContext);
            Object value = getToConverter(types[i]).convert(projected);
            injectors[i].set(value);
        }
        return target.conclude();
    }

    static Supplier supplier(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTargetDescriptor descriptor,
            List<Expression<?>> projection
    ) {
        return new Supplier(paths, types, descriptor, projection);
    }

    static final class Supplier implements DataSerializable {

        private QueryPath[] paths;
        private QueryDataType[] types;

        private UpsertTargetDescriptor descriptor;

        private List<Expression<?>> projection;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                QueryPath[] paths,
                QueryDataType[] types,
                UpsertTargetDescriptor descriptor,
                List<Expression<?>> projection
        ) {
            this.paths = paths;
            this.types = types;
            this.descriptor = descriptor;
            this.projection = projection;
        }

        Projector get(ExpressionEvalContext evalContext) {
            return new Projector(
                    paths,
                    types,
                    descriptor.create(evalContext.getSerializationService()),
                    projection,
                    evalContext
            );
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(paths.length);
            for (QueryPath path : paths) {
                out.writeObject(path);
            }
            out.writeInt(types.length);
            for (QueryDataType type : types) {
                out.writeObject(type);
            }
            out.writeObject(descriptor);
            out.writeObject(projection);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            paths = new QueryPath[in.readInt()];
            for (int i = 0; i < paths.length; i++) {
                paths[i] = in.readObject();
            }
            types = new QueryDataType[in.readInt()];
            for (int i = 0; i < types.length; i++) {
                types[i] = in.readObject();
            }
            descriptor = in.readObject();
            projection = in.readObject();
        }
    }
}
