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
import com.hazelcast.core.IMap;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ArrayMap;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.jet.impl.util.Util;

import javax.annotation.Nonnull;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static com.hazelcast.jet.impl.connector.HazelcastWriters.handleInstanceNotActive;
import static com.hazelcast.jet.impl.util.ExceptionUtil.sneakyThrow;

public final class WriteMapP<K, V> implements Processor {
    private final AtomicInteger numParallelOps = new AtomicInteger();
    private final AtomicReference<Throwable> firstFailure = new AtomicReference<>();
    private final HazelcastInstance instance;
    private final String mapName;
    private final boolean isLocal;
    private final BiConsumer<Object, Throwable> callback = (r, t) -> {
        if (t != null) {
            firstFailure.compareAndSet(null, t);
        }
        numParallelOps.decrementAndGet();
    };

    private int parallelOpsLimit;
    private IMap<K, V> map;

    private WriteMapP(HazelcastInstance instance, String mapName) {
        this.instance = instance;
        this.mapName = mapName;
        this.isLocal = ImdgUtil.isMemberInstance(instance);
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        // We distribute the local limit to processors. If there's 1 local processor, it
        // will take the entire maximum. If there are many local processors, each will
        // get 1. The putAll operation is already bulky, it doesn't help to have many in
        // parallel.
        map = instance.getMap(mapName);
        parallelOpsLimit = Math.max(1, Supplier.MAX_LOCAL_PARALLEL_OPS / context.localParallelism());
    }

    @Override
    public boolean tryProcess() {
        checkFailure();
        return true;
    }

    @Override
    public void process(int ordinal, @Nonnull Inbox inbox) {
        checkFailure();
        if (Util.tryIncrement(numParallelOps, 1, parallelOpsLimit)) {
            ArrayMap<K, V> inboxAsMap = new ArrayMap<>(inbox.size());
            inbox.drain(inboxAsMap::add);
            ImdgUtil.mapPutAllAsync(map, inboxAsMap)
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
            return numParallelOps.get() == 0;
        } finally {
            checkFailure();
        }
    }

    public static class Supplier<K, V> extends AbstractHazelcastConnectorSupplier {
        private static final long serialVersionUID = 1L;
        private static final int MAX_LOCAL_PARALLEL_OPS = 8;

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
