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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceAware;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class UpdateProcessor extends AbstractProcessor {
    private final String mapName;
    private final KvRowProjector.Supplier mapEntryProjectorSupplier;
    private final KvProjector.Supplier kvProjectorSupplier;
    private final int[] updateColumns;

    private MapProxyImpl<Object, Object> map;
    private ExpressionEvalContext evalContext;

    private UpdateProcessor(
            String mapName,
            KvRowProjector.Supplier mapEntryProjectorSupplier,
            KvProjector.Supplier kvProjectorSupplier,
            int[] updateColumns) {
        this.mapName = mapName;
        this.mapEntryProjectorSupplier = mapEntryProjectorSupplier;
        this.kvProjectorSupplier = kvProjectorSupplier;
        this.updateColumns = updateColumns;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = (MapProxyImpl<Object, Object>) context.jetInstance().getMap(mapName);
        evalContext = SimpleExpressionEvalContext.from(context);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        int columnCount = mapEntryProjectorSupplier.columnCount();

        for (Object[] row; (row = (Object[]) inbox.peek()) != null; ) {
            inbox.remove();
            Object key = row[0];
            map.submitToKey(
                    key,
                    new UpdateEntryProcessor(
                            mapEntryProjectorSupplier, kvProjectorSupplier, evalContext, row, columnCount, updateColumns));
        }
    }

    private static class UpdateEntryProcessor
            implements EntryProcessor<Object, Object, Object>, SerializationServiceAware, DataSerializable {

        private KvRowProjector.Supplier rowProjector;
        private KvProjector.Supplier kvProjector;
        private List<Object> arguments;
        private Object[] updates;
        private int columnCount;
        private int[] updateColumns;

        private transient ExpressionEvalContext evalContext;
        private InternalSerializationService serializationService;
        private transient Extractors extractors;

        @SuppressWarnings("unused") // for deserialization
        private UpdateEntryProcessor() { }

        UpdateEntryProcessor(
                KvRowProjector.Supplier rowProjector,
                KvProjector.Supplier kvProjector,
                ExpressionEvalContext evalContext,
                Object[] updates,
                int columnCount,
                int[] updateColumns) {
            this.rowProjector = rowProjector;
            this.kvProjector = kvProjector;
            this.evalContext = evalContext;
            this.arguments = evalContext.getArguments();
            this.updates = updates;
            this.columnCount = columnCount;
            this.updateColumns = updateColumns;
        }

        @Override
        public void setSerializationService(SerializationService serializationService) {
            this.evalContext =
                    new SimpleExpressionEvalContext(arguments, (InternalSerializationService) serializationService);
            this.serializationService = (InternalSerializationService) serializationService;
            this.extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
        }

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            Object[] project = rowProjector.get(evalContext, extractors).project(entry);
            if (updateColumns.length == 0) {
                // 0 - __key
                // 1 - old this
                // 2 - new this
                project[1] = updates[2];
            } else {
                int i = 0;
                for (int index : updateColumns) {
                    project[index] = updates[columnCount + i];
                    i++;
                }
            }
            KvProjector kvProjector = this.kvProjector.get(serializationService);
            Map.Entry<Object, Object> newEntry = kvProjector.project(project);
            // we have to call setValue explicitly otherwise entry processor
            // won't update entry. See javadoc of #process method
            entry.setValue(newEntry.getValue());
            return entry;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(rowProjector);
            out.writeObject(kvProjector);
            out.writeObject(arguments);
            out.writeObject(updateColumns);
            out.writeObject(updates);
            out.writeInt(columnCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            rowProjector = in.readObject();
            kvProjector = in.readObject();
            arguments = in.readObject();
            updateColumns = in.readObject();
            updates = in.readObject();
            columnCount = in.readInt();
        }
    }

    public static final class Supplier implements SupplierEx<Processor>, DataSerializable {

        private String mapName;
        private KvRowProjector.Supplier kvRowProjectorSupplier;
        private KvProjector.Supplier kvProjectorSupplier;
        private int[] updateColumns;

        @SuppressWarnings("unused") // for deserialization
        Supplier() { }

        public Supplier(
                String mapName,
                KvRowProjector.Supplier kvRowProjectorSupplier,
                KvProjector.Supplier kvProjectorSupplier,
                int[] updateColumns) {
            this.mapName = mapName;
            this.kvRowProjectorSupplier = kvRowProjectorSupplier;
            this.kvProjectorSupplier = kvProjectorSupplier;
            this.updateColumns = updateColumns;
        }

        @Override
        public Processor getEx() {
            return new UpdateProcessor(mapName, kvRowProjectorSupplier, kvProjectorSupplier, updateColumns);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeObject(kvRowProjectorSupplier);
            out.writeObject(kvProjectorSupplier);
            out.writeIntArray(updateColumns);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            kvRowProjectorSupplier = in.readObject();
            kvProjectorSupplier = in.readObject();
            updateColumns = in.readIntArray();
        }
    }
}
