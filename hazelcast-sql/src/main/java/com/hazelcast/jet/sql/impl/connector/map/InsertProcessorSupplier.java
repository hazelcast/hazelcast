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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.execution.init.Contexts;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcCtx;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.sql.impl.row.JetSqlRow;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.sql.impl.QueryException;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.security.Permission;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.security.permission.ActionConstants.ACTION_CREATE;
import static com.hazelcast.security.permission.ActionConstants.ACTION_PUT;
import static java.util.Collections.singletonList;

final class InsertProcessorSupplier implements ProcessorSupplier, DataSerializable {

    private String mapName;
    private KvProjector.Supplier projectorSupplier;

    private transient InternalSerializationService serializationService;

    @SuppressWarnings("unused")
    private InsertProcessorSupplier() {
    }

    InsertProcessorSupplier(String mapName, KvProjector.Supplier projectorSupplier) {
        this.mapName = mapName;
        this.projectorSupplier = projectorSupplier;
    }

    @Override
    public void init(@Nonnull Context context) {
        serializationService = ((Contexts.ProcSupplierCtx) context).serializationService();
    }

    @Nonnull
    @Override
    public Collection<? extends Processor> get(int count) {
        assert count == 1;

        return singletonList(new InsertP(mapName, projectorSupplier.get(serializationService)));
    }

    @Override
    public List<Permission> permissions() {
        return singletonList(new MapPermission(mapName, ACTION_CREATE, ACTION_PUT));
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeString(mapName);
        out.writeObject(projectorSupplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        mapName = in.readString();
        projectorSupplier = in.readObject();
    }

    private static final class InsertP extends AbstractProcessor {

        private static final int MAX_IN_FLIGHT_INSERTS = 16;

        private final String mapName;
        private final KvProjector projector;
        private final Set<Object> seenKeys = new HashSet<>();
        private final Deque<CompletableFuture<Object>> inFlightInserts = new ArrayDeque<>(MAX_IN_FLIGHT_INSERTS);

        private MapProxyImpl<Object, Object> map;
        private long maxAccumulatedKeys;
        private long numberOfInsertedEntries;
        private SerializationService serializationService;

        private InsertP(String mapName, KvProjector projector) {
            this.mapName = mapName;
            this.projector = projector;
        }

        @Override
        protected void init(@Nonnull Context context) {
            map = (MapProxyImpl<Object, Object>) context.hazelcastInstance().getMap(mapName);
            maxAccumulatedKeys = context.maxProcessorAccumulatedRecords();
            serializationService = ((ProcCtx) context).serializationService();
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object row) {
            if (!tryFlushQueue() && isQueueFull()) {
                return false;
            }

            Entry<Object, Object> entry = projector.project((JetSqlRow) row);
            if (!seenKeys.add(entry.getKey())) {
                throw QueryException.error("Duplicate key");
            }
            if (seenKeys.size() > maxAccumulatedKeys) {
                throw new AccumulationLimitExceededException();
            }
            inFlightInserts.add(map.putIfAbsentAsync(entry.getKey(), entry.getValue()));
            return true;
        }

        @Override
        public boolean complete() {
            return tryFlushQueue() && tryEmit(new JetSqlRow(serializationService, new Object[]{numberOfInsertedEntries}));
        }

        private boolean isQueueFull() {
            return inFlightInserts.size() == MAX_IN_FLIGHT_INSERTS;
        }

        private boolean tryFlushQueue() {
            CompletableFuture<Object> future;
            while ((future = inFlightInserts.peek()) != null) {
                if (!future.isDone()) {
                    return false;
                } else {
                    Object previousValue;
                    try {
                        previousValue = future.get();
                    } catch (Throwable e) {
                        throw new JetException("INSERT operation completed exceptionally: " + e, e);
                    }

                    inFlightInserts.remove();

                    if (previousValue == null) {
                        numberOfInsertedEntries++;
                    } else {
                        throw QueryException.error("Duplicate key");
                    }
                }
            }
            return true;
        }
    }
}
