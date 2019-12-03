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
import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Inbox;
import com.hazelcast.jet.core.Outbox;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.impl.connector.HazelcastWriters.ArrayMap;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.function.Consumer;

import static java.lang.Integer.max;

public final class WriteMapP<K, V> extends AsyncHazelcastWriterP {

    private static final int BUFFER_LIMIT = 1024;
    private final String mapName;
    private final ArrayMap<K, V> buffer = new ArrayMap<>(EdgeConfig.DEFAULT_QUEUE_SIZE);

    private IMap<K, V> map;
    private Consumer<Entry<K, V>> addToBuffer = buffer::add;

    private WriteMapP(HazelcastInstance instance, int maxParallelAsyncOps, String mapName) {
        super(instance, maxParallelAsyncOps);
        this.mapName = mapName;
    }

    @Override
    public void init(@Nonnull Outbox outbox, @Nonnull Context context) {
        map = instance().getMap(mapName);
    }

    @Override
    protected void processInternal(Inbox inbox) {
        if (buffer.size() < BUFFER_LIMIT) {
            inbox.drain(addToBuffer);
        }
        submitPending();
    }

    @Override
    protected boolean flushInternal() {
        return submitPending();
    }

    private boolean submitPending() {
        if (buffer.isEmpty()) {
            return true;
        }
        if (!tryAcquirePermit()) {
            return false;
        }
        setCallback(ImdgUtil.mapPutAllAsync(map, buffer));
        buffer.clear();
        return true;
    }

    public static class Supplier<K, V> extends AbstractHazelcastConnectorSupplier {
        private static final long serialVersionUID = 1L;

        // use a conservative max parallelism to prevent overloading
        // the cluster with putAll operations
        private static final int MAX_PARALLELISM = 16;

        private final String mapName;
        private int maxParallelAsyncOps;

        public Supplier(String clientXml, String mapName) {
            super(clientXml);
            this.mapName = mapName;
        }

        @Override
        public void init(@Nonnull Context context) {
            super.init(context);
            maxParallelAsyncOps = max(1, MAX_PARALLELISM / context.localParallelism());
        }

        @Override
        protected Processor createProcessor(HazelcastInstance instance) {
            return new WriteMapP<>(instance, maxParallelAsyncOps, mapName);
        }
    }
}
