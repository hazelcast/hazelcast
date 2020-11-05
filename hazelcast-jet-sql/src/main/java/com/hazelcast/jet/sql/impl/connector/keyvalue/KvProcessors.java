/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

public final class KvProcessors {

    private KvProcessors() {
    }

    public static ProcessorSupplier rowProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            QueryTargetDescriptor keyDescriptor,
            QueryTargetDescriptor valueDescriptor,
            Expression<Boolean> predicate,
            List<Expression<?>> projection
    ) {
        return new RowProjectorProcessorSupplier(paths, types, keyDescriptor, valueDescriptor, predicate, projection);
    }

    public static ProcessorSupplier entryProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor
    ) {
        return new EntryProjectorProcessorSupplier(paths, types, keyDescriptor, valueDescriptor);
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class RowProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private QueryPath[] paths;
        private QueryDataType[] types;

        private QueryTargetDescriptor keyDescriptor;
        private QueryTargetDescriptor valueDescriptor;

        private Expression<Boolean> predicate;
        private List<Expression<?>> projection;

        private transient InternalSerializationService serializationService;
        private transient Extractors extractors;

        @SuppressWarnings("unused")
        private RowProjectorProcessorSupplier() {
        }

        RowProjectorProcessorSupplier(
                QueryPath[] paths,
                QueryDataType[] types,
                QueryTargetDescriptor keyDescriptor,
                QueryTargetDescriptor valueDescriptor,
                Expression<Boolean> predicate,
                List<Expression<?>> projection
        ) {
            this.paths = paths;
            this.types = types;

            this.keyDescriptor = keyDescriptor;
            this.valueDescriptor = valueDescriptor;

            this.predicate = predicate;
            this.projection = projection;
        }

        @Override
        public void init(@Nonnull Context context) {
            serializationService = ((ProcSupplierCtx) context).serializationService();
            extractors = Extractors.newBuilder(serializationService).build();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResettableSingletonTraverser<Object[]> traverser = new ResettableSingletonTraverser<>();
                KvRowProjector projector = new KvRowProjector(
                        paths,
                        types,
                        keyDescriptor.create(serializationService, extractors, true),
                        valueDescriptor.create(serializationService, extractors, false),
                        predicate,
                        projection
                );
                Processor processor = new TransformP<Entry<Object, Object>, Object[]>(entry -> {
                    traverser.accept(projector.project(entry));
                    return traverser;
                });
                processors.add(processor);
            }
            return processors;
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
            out.writeObject(predicate);
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
            keyDescriptor = in.readObject();
            valueDescriptor = in.readObject();
            predicate = in.readObject();
            projection = in.readObject();
        }
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class EntryProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private QueryPath[] paths;
        private QueryDataType[] types;

        private UpsertTargetDescriptor keyDescriptor;
        private UpsertTargetDescriptor valueDescriptor;

        private transient InternalSerializationService serializationService;

        @SuppressWarnings("unused")
        private EntryProjectorProcessorSupplier() {
        }

        EntryProjectorProcessorSupplier(
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

        @Override
        public void init(@Nonnull Context context) {
            serializationService = ((ProcSupplierCtx) context).serializationService();
        }

        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            List<Processor> processors = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                ResettableSingletonTraverser<Object> traverser = new ResettableSingletonTraverser<>();
                KvProjector projector = new KvProjector(
                        paths,
                        types,
                        keyDescriptor.create(serializationService),
                        valueDescriptor.create(serializationService)
                );
                Processor processor = new TransformP<Object[], Object>(row -> {
                    traverser.accept(projector.project(row));
                    return traverser;
                });
                processors.add(processor);
            }
            return processors;
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
