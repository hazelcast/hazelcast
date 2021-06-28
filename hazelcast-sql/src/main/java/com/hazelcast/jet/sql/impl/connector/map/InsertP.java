/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.impl.execution.init.Contexts.ProcSupplierCtx;
import com.hazelcast.jet.impl.memory.AccumulationLimitExceededException;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvProjector;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.sql.impl.QueryException;

import javax.annotation.Nonnull;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

class InsertP extends AbstractProcessor {

    private static final int MAX_IN_FLIGHT_INSERTS = 16;

    private final String mapName;
    private final KvProjector.Supplier projectorSupplier;
    private final Set<Object> seenKeys = new HashSet<>();
    private final Deque<CompletableFuture<Object>> inFlightInserts = new ArrayDeque<>(MAX_IN_FLIGHT_INSERTS);

    private KvProjector projector;
    private MapProxyImpl<Object, Object> map;
    private long maxAccumulatedKeys;
    private long numberOfInsertedEntries;

    InsertP(String mapName, KvProjector.Supplier projectorSupplier) {
        this.mapName = mapName;
        this.projectorSupplier = projectorSupplier;
    }

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        map = (MapProxyImpl<Object, Object>) context.hazelcastInstance().getMap(mapName);
        maxAccumulatedKeys = context.maxProcessorAccumulatedRecords();
        InternalSerializationService serializationService = ((ProcSupplierCtx) context).serializationService();
        projector = projectorSupplier.get(serializationService);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object row) {
        if (isQueueFull()) {
            if (!tryFlushQueue() && isQueueFull()) {
                return false;
            }
        }

        Entry<Object, Object> entry = projector.project((Object[]) row);
        if (entry.getKey() == null) {
            throw QueryException.error("Key cannot be null");
        }
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
        return tryFlushQueue() && tryEmit(new Object[]{numberOfInsertedEntries});
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
