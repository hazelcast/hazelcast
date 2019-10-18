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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ArrayMap;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.impl.connector.HazelcastWriters.handleInstanceNotActive;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public final class WriteMapP<K, V> implements Processor {
    // This is a cooperative processor it will use maximum
    // local parallelism by default. We also use an incoming
    // local partitioned edge so each processor deals with a
    // subset of the partitions. We want to limit the number of
    // in flight operations since putAll operation can be slow
    // and bulky, otherwise we may face timeouts.
    private final AtomicBoolean pendingOp = new AtomicBoolean();
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private final HazelcastInstance instance;
    private final String mapName;
    private final boolean isLocal;
    private final ArrayMap<K, V> buffer = new ArrayMap<>(EdgeConfig.DEFAULT_QUEUE_SIZE);
    private final BiConsumer<Object, Throwable> callback = (r, t) -> {
        if (t != null) {
            firstFailure.compareAndSet(null, t);
        }
        buffer.clear();
        pendingOp.set(false);
    };


    private IMap<K, V> map;

    private WriteMapP(HazelcastInstance instance, String mapName) {
        this.instance = instance;
        this.mapName = mapName;
        this.isLocal = ImdgUtil.isMemberInstance(instance);
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        map = instance.getMap(mapName);
    }

    @Override
    public boolean tryProcess() {
        checkFailure();
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        checkFailure();
        if (pendingOp.compareAndSet(false, true)) {
            inbox.drain(buffer::add);
            ImdgUtil.mapPutAllAsync(map, buffer)
                    .whenComplete(callback);
        }
    }

    @Override
    public boolean tryProcessWatermark(@Nonnull Watermark watermark) {
        return true;
    }

    @Override
    public boolean saveToSnapshot() {
        return ensureAllSuccessfullyWritten();
    }

    @Override
    public boolean complete() {
        return ensureAllSuccessfullyWritten();
    }

    private void checkFailure() {
        Throwable failure = firstFailure.get();
        if (failure != null) {
            if (failure instanceof HazelcastInstanceNotActiveException) {
                failure = handleInstanceNotActive((HazelcastInstanceNotActiveException) failure, isLocal);
            }
            throw sneakyThrow(failure);
        }
    }

    private boolean ensureAllSuccessfullyWritten() {
        try {
            return !pendingOp.get();
        } finally {
            checkFailure();
        }
    }

    public static class Supplier<K, V> extends AbstractHazelcastConnectorSupplier {
        private static final long serialVersionUID = 1L;

        private final String mapName;

        public Supplier(String clientXml, String mapName) {
            super(clientXml);
            this.mapName = mapName;
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance) {
            return new WriteMapP<>(instance, mapName);
        }
    }
}
