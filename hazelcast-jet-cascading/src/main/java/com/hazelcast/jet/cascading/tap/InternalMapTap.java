/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.cascading.tap;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.TupleEntrySchemeCollector;
import cascading.tuple.TupleEntrySchemeIterator;
import cascading.util.SingleCloseableInputIterator;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.jet.cascading.JetFlowProcess;
import com.hazelcast.jet2.JetEngineConfig;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.impl.IMapReader;
import com.hazelcast.jet2.impl.IMapWriter;
import com.hazelcast.map.impl.MapService;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;

public class InternalMapTap extends InternalJetTap {

    private static final Object CLIENT_LOCK = new Object();
    private static HazelcastInstance client;

    private final String mapName;

    public InternalMapTap(String mapName, Scheme<JetEngineConfig, Iterator<Map.Entry>, Outbox, ?, ?> scheme) {
        this(mapName, scheme, SinkMode.KEEP);
    }

    public InternalMapTap(String mapName,
                          Scheme<JetEngineConfig, Iterator<Map.Entry>, Outbox, ?, ?> scheme,
                          SinkMode sinkMode) {
        super(scheme, sinkMode);
        this.mapName = mapName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public TupleEntryIterator openForRead(FlowProcess<? extends JetEngineConfig> flowProcess,
                                          Iterator<Map.Entry> input) throws IOException {

        if (input == null) {
            HazelcastInstance instance = ((JetFlowProcess) flowProcess).getHazelcastInstance();
            IMap map = findIMap(instance);
            if (map == null) {
                throw new IOException("Could not find map " + mapName);
            }
            input = new TreeMap(map).entrySet().iterator();
        }
        return new TupleEntrySchemeIterator<>(flowProcess, getScheme(),
                new SingleCloseableInputIterator(makeCloseable(input)));
    }

    @Override
    public TupleEntryCollector openForWrite(FlowProcess<? extends JetEngineConfig> flowProcess,
                                            Outbox outbox) throws IOException {
        if (outbox != null) {
            return new SettableTupleEntryCollector<>(flowProcess, getScheme(), outbox);
        }

        HazelcastInstance instance = ((JetFlowProcess) flowProcess).getHazelcastInstance();
        final IMap map = instance.getMap(mapName);
        return new TupleEntrySchemeCollector<>(flowProcess, getScheme(), new Outbox() {
            @Override
            public void add(Object item) {
                Entry entry = (Entry) item;
                map.put(entry.getKey(), entry.getValue());
            }

            @Override
            public void add(int ordinal, Object item) {
                Entry entry = (Entry) item;
                map.put(entry.getKey(), entry.getValue());
            }

            @Override
            public boolean isHighWater() {
                return false;
            }

            @Override
            public boolean isHighWater(int ordinal) {
                return false;
            }
        });
    }

    @Override
    public boolean createResource(JetEngineConfig conf) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean deleteResource(JetEngineConfig conf) throws IOException {
        HazelcastInstance client = getHazelcastInstance();
        IMap map = findIMap(client);
        if (map == null) {
            return false;
        }

        client.getExecutorService(mapName).executeOnAllMembers(new DestroyMap(mapName));
        return true;
    }

    @Override
    public boolean resourceExists(JetEngineConfig conf) throws IOException {
        //TODO: config should be refactored
        HazelcastInstance client = getHazelcastInstance();
        return findIMap(client) != null;
    }

    protected HazelcastInstance getHazelcastInstance() {
        //TODO: used for speeding up tests, should be fixed after config refactor
        synchronized (CLIENT_LOCK) {
            if (client == null) {
                client = HazelcastClient.newHazelcastClient();
            }
            return client;
        }
    }

    @Override
    public long getModifiedTime(JetEngineConfig conf) throws IOException {
        HazelcastInstance client = getHazelcastInstance();
        if (findIMap(client) == null) {
            throw new IOException("Could not find " + mapName);
        }
        Map<Member, Future<Long>> memberFutureMap = client.getExecutorService(mapName)
                                                          .submitToAllMembers(new LastModifiedTime(mapName));

        long lastModified = 0;
        for (Future<Long> longFuture : memberFutureMap.values()) {
            try {
                if (longFuture.get() > lastModified) {
                    lastModified = longFuture.get();
                }
            } catch (InterruptedException | ExecutionException e) {
                throw rethrow(e);
            }
        }
        return lastModified;
    }

    @Override
    public String getIdentifier() {
        return mapName;
    }

    @Override
    public boolean isSource() {
        return true;
    }

    @Override
    public boolean isSink() {
        return true;
    }

    @Override
    public ProcessorMetaSupplier getSource() {
        return IMapReader.supplier(mapName);
    }

    @Override
    public ProcessorMetaSupplier getSink() {
        return IMapWriter.supplier(mapName);
    }

    private IMap findIMap(HazelcastInstance instance) {
        for (DistributedObject object : instance.getDistributedObjects()) {
            if (object instanceof IMap && object.getName().equals(mapName)) {
                return (IMap) object;
            }
        }
        return null;
    }

    private static final class LastModifiedTime implements Callable<Long>, HazelcastInstanceAware, Serializable {

        private static final long serialVersionUID = 1L;
        private final String mapName;
        private transient HazelcastInstance hazelcastInstance;

        private LastModifiedTime(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public Long call() throws Exception {
            return hazelcastInstance.getMap(mapName).getLocalMapStats().getLastUpdateTime();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }

    private static final class DestroyMap implements Runnable, HazelcastInstanceAware, Serializable {

        private static final long serialVersionUID = 1L;
        private final String mapName;
        private transient HazelcastInstance hazelcastInstance;

        private DestroyMap(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public void run() {
            hazelcastInstance.getDistributedObject(MapService.SERVICE_NAME, mapName).destroy();
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hazelcastInstance = hazelcastInstance;
        }
    }
}
