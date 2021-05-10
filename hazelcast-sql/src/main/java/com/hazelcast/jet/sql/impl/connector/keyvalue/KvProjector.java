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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.inject.UpsertInjector;
import com.hazelcast.jet.sql.impl.inject.UpsertTarget;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.processors.JetSqlRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.io.IOException;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.type.converter.ToConverters.getToConverter;

/**
 * A utility to convert a row represented as {@link JetSqlRow} to a
 * key-value entry represented as {@code Entry<Object, Object>}.
 * <p>
 * {@link KvRowProjector} does the reverse.
 */
class KvProjector {

    private final InternalSerializationService serializationService;
    private final QueryDataType[] types;

    private final UpsertTarget keyTarget;
    private final UpsertTarget valueTarget;

    private final UpsertInjector[] injectors;

    KvProjector(
            InternalSerializationService serializationService,
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTarget keyTarget,
            UpsertTarget valueTarget
    ) {
        this.serializationService = serializationService;
        this.types = types;

        this.keyTarget = keyTarget;
        this.valueTarget = valueTarget;

        this.injectors = createInjectors(paths, types, keyTarget, valueTarget);
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

    Entry<Object, Object> project(JetSqlRow row) {
        keyTarget.init();
        valueTarget.init();
        for (int i = 0; i < row.getFieldCount(); i++) {
            Object value = getToConverter(types[i]).convert(row.get(serializationService, i));
            injectors[i].set(value);
        }
        return entry(keyTarget.conclude(), valueTarget.conclude());
    }

    public static Supplier supplier(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor
    ) {
        return new Supplier(paths, types, keyDescriptor, valueDescriptor);
    }

    public static final class Supplier implements DataSerializable {

        private QueryPath[] paths;
        private QueryDataType[] types;

        private UpsertTargetDescriptor keyDescriptor;
        private UpsertTargetDescriptor valueDescriptor;

        @SuppressWarnings("unused")
        private Supplier() {
        }

        private Supplier(
                QueryPath[] paths,
                QueryDataType[] types,
                UpsertTargetDescriptor keyDescriptor,
                UpsertTargetDescriptor valueDescriptor
        ) {
            this.paths = paths;
            this.types = types;
            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;
        }

        public KvProjector get(InternalSerializationService serializationService) {
            return new KvProjector(
                    serializationService,
                    paths,
                    types,
                    keyDescriptor.create(serializationService),
                    valueDescriptor.create(serializationService)
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
        }
    }
}
