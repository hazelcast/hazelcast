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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Map.Entry;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;

/**
 * A utility to convert a row represented as {@link JetSqlRow} to a
 * key-value entry represented as {@code Entry<Object, Object>}.
 * <p>
 * {@link KvRowProjector} does the reverse.
 */
public class KvProjector {

    private final QueryDataType[] types;

    private final UpsertTarget keyTarget;
    private final UpsertTarget valueTarget;

    private final UpsertInjector[] injectors;

    private final boolean failOnNulls;

    KvProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTarget keyTarget,
            UpsertTarget valueTarget,
            boolean failOnNulls
    ) {
        checkTrue(paths.length == types.length, "paths.length != types.length");
        this.types = types;

        this.keyTarget = keyTarget;
        this.valueTarget = valueTarget;

        this.injectors = createInjectors(paths, types, keyTarget, valueTarget);

        this.failOnNulls = failOnNulls;
    }

    private static UpsertInjector[] createInjectors(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTarget keyTarget,
            UpsertTarget valueTarget
    ) {
        UpsertInjector[] injectors = new UpsertInjector[paths.length];
        for (int i = 0; i < paths.length; i++) {
            UpsertTarget target = paths[i].isKey() ? keyTarget : valueTarget;
            injectors[i] = target.createInjector(paths[i].getPath(), types[i]);
        }
        return injectors;
    }

    public Entry<Object, Object> project(JetSqlRow row) {
        keyTarget.init();
        valueTarget.init();
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object value = getToConverter(types[i]).convert(row.get(i));
            injectors[i].set(value);
        }

        Object key = keyTarget.conclude();
        if (key == null && failOnNulls) {
            throw QueryException.error("Cannot write NULL to '__key' field. " +
                    "Note that NULL is used also if your INSERT/SINK command doesn't write to '__key' at all.");
        }

        Object value = valueTarget.conclude();
        if (value == null && failOnNulls) {
            throw QueryException.error("Cannot write NULL to 'this' field. " +
                    "Note that NULL is used also if your INSERT/SINK command doesn't write to 'this' at all.");
        }

        return entry(key, value);
    }

    public static Supplier supplier(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            boolean failOnNulls
    ) {
        return new Supplier(paths, types, keyDescriptor, valueDescriptor, failOnNulls);
    }

    public static final class Supplier implements DataSerializable {

        private QueryPath[] paths;
        private QueryDataType[] types;

        private UpsertTargetDescriptor keyDescriptor;
        private UpsertTargetDescriptor valueDescriptor;

        private boolean failOnNulls;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                QueryPath[] paths,
                QueryDataType[] types,
                UpsertTargetDescriptor keyDescriptor,
                UpsertTargetDescriptor valueDescriptor,
                boolean failOnNulls
        ) {
            this.paths = paths;
            this.types = types;
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;
            this.failOnNulls = failOnNulls;
        }

        public KvProjector get(InternalSerializationService serializationService) {
            return new KvProjector(
                    paths,
                    types,
                    keyDescriptor.create(serializationService),
                    valueDescriptor.create(serializationService),
                    failOnNulls
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
            out.writeObject(keyDescriptor);
            out.writeObject(valueDescriptor);
            out.writeBoolean(failOnNulls);
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
            keyDescriptor = in.readObject();
            valueDescriptor = in.readObject();
            failOnNulls = in.readBoolean();
        }
    }
}
