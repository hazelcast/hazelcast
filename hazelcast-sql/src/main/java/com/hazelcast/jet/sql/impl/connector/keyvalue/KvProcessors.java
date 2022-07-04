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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.ResettableSingletonTraverser;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.processor.TransformP;
import com.hazelcast.jet.sql.impl.inject.UpsertTargetDescriptor;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.type.QueryDataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class KvProcessors {

    private KvProcessors() {
    }

    /**
     * Returns a supplier of processors that convert a row represented as
     * {@link JetSqlRow} to an entry represented as {@code Entry<Object,
     * Object>}.
     */
    public static ProcessorSupplier entryProjector(
            QueryPath[] paths,
            QueryDataType[] types,
            UpsertTargetDescriptor keyDescriptor,
            UpsertTargetDescriptor valueDescriptor,
            boolean failOnNulls
    ) {
        return new EntryProjectorProcessorSupplier(KvProjector.supplier(
                paths,
                types,
                keyDescriptor,
                valueDescriptor,
                failOnNulls
        ));
    }

    @SuppressFBWarnings(
            value = {"SE_BAD_FIELD", "SE_NO_SERIALVERSIONID"},
            justification = "the class is never java-serialized"
    )
    private static final class EntryProjectorProcessorSupplier implements ProcessorSupplier, DataSerializable {

        private KvProjector.Supplier projectorSupplier;

        private transient InternalSerializationService serializationService;

        @SuppressWarnings("unused")
        private EntryProjectorProcessorSupplier() {
        }

        EntryProjectorProcessorSupplier(KvProjector.Supplier projectorSupplier) {
            this.projectorSupplier = projectorSupplier;
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
                KvProjector projector = projectorSupplier.get(serializationService);
                Processor processor = new TransformP<JetSqlRow, Object>(row -> {
                    traverser.accept(projector.project(row));
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
    }
}
