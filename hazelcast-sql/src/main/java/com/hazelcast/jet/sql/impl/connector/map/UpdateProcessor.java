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
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.sql.impl.SimpleExpressionEvalContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvRowProjector;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.expression.ExpressionEvalContext;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.Map;

public final class UpdateProcessor extends AbstractProcessor {
    private final String mapName;
    private final KvRowProjector.Supplier mapEntryProjectorSupplier;
    private final int[] updateColumns;

    private MapProxyImpl<Object, Object> map;
    private KvRowProjector kvRowProjector;

    private UpdateProcessor(String mapName, KvRowProjector.Supplier mapEntryProjectorSupplier, int[] updateColumns) {
        this.mapName = mapName;
        this.mapEntryProjectorSupplier = mapEntryProjectorSupplier;
        this.updateColumns = updateColumns;
    }

    @Override
    public void init(@Nonnull Context context) {
        map = (MapProxyImpl<Object, Object>) context.jetInstance().getMap(mapName);
        ExpressionEvalContext evalContext = SimpleExpressionEvalContext.from(context);
        Extractors extractors = Extractors.newBuilder(evalContext.getSerializationService()).build();
        kvRowProjector = mapEntryProjectorSupplier.get(evalContext, extractors);
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        int columnCount = mapEntryProjectorSupplier.columnCount();

        for (Object[] row; (row = (Object[]) inbox.peek()) != null; ) {
            inbox.remove();
            Object key = row[0];
            Object value = map.get(key);
            Map.Entry<Object, Object> entry = Util.entry(key, value);
            Object[] project = kvRowProjector.project(entry);
            if (updateColumns.length == 0) {
                // 0 - __key
                // 1 - old this
                // 2 - new this
                project[1] = row[2];
            } else {
                int i = 0;
                for (int index : updateColumns) {
                    project[index] = row[columnCount + i];
                    i++;
                }
            }
            getOutbox().offer(project);
        }
    }

    public static final class Supplier implements SupplierEx<Processor>, DataSerializable {

        private String mapName;
        private KvRowProjector.Supplier projectorSupplier;
        private int[] updateColumns;

        @SuppressWarnings("unused") // for deserialization
        Supplier() { }

        public Supplier(String mapName, KvRowProjector.Supplier projectorSupplier, int[] updateColumns) {
            this.mapName = mapName;
            this.projectorSupplier = projectorSupplier;
            this.updateColumns = updateColumns;
        }

        @Override
        public Processor getEx() {
            return new UpdateProcessor(mapName, projectorSupplier, updateColumns);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeString(mapName);
            out.writeObject(projectorSupplier);
            out.writeIntArray(updateColumns);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            mapName = in.readString();
            projectorSupplier = in.readObject();
            updateColumns = in.readIntArray();
        }
    }
}
