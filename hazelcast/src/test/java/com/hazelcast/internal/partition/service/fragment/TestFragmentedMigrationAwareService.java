/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.service.fragment;

import com.hazelcast.config.ServiceConfig;
import com.hazelcast.internal.partition.ChunkedMigrationAwareService;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.partition.service.TestAbstractMigrationAwareService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class TestFragmentedMigrationAwareService extends TestAbstractMigrationAwareService<String>
        implements ChunkedMigrationAwareService {

    public static final String SERVICE_NAME = TestFragmentedMigrationAwareService.class.getSimpleName();

    public static ServiceConfig createServiceConfig(int backupCount) {
        return new ServiceConfig()
                .setEnabled(true).setName(TestFragmentedMigrationAwareService.SERVICE_NAME)
                .setClassName(TestFragmentedMigrationAwareService.class.getName())
                .addProperty(BACKUP_COUNT_PROP, String.valueOf(backupCount));
    }

    private final ConcurrentMap<Key, Integer> data = new ConcurrentHashMap<>();

    int inc(String name, int partitionId) {
        Key key = new Key(name, partitionId);
        Integer count = data.get(key);
        if (count == null) {
            count = 1;
        } else {
            count++;
        }
        data.put(key, count);
        return count;
    }

    void put(String name, int partitionId, int value) {
        data.put(new Key(name, partitionId), value);
    }

    @Override
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        if (event.getReplicaIndex() > backupCount) {
            return Collections.emptySet();
        }

        Set<ServiceNamespace> knownNamespaces = new HashSet<ServiceNamespace>();
        for (Key key : data.keySet()) {
            knownNamespaces.add(new TestServiceNamespace(key.name));
        }
        return Collections.unmodifiableCollection(knownNamespaces);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return namespace instanceof TestServiceNamespace;
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event, Collection<ServiceNamespace> namespaces) {
        if (event.getReplicaIndex() > backupCount || namespaces.isEmpty()) {
            return null;
        }

        Collection<ServiceNamespace> knownNamespaces = getAllServiceNamespaces(event);
        Map<TestServiceNamespace, Integer> values = new HashMap<TestServiceNamespace, Integer>(namespaces.size());
        for (ServiceNamespace ns : namespaces) {
            assertThat(ns, isIn(knownNamespaces));

            TestServiceNamespace testNs = (TestServiceNamespace) ns;
            Integer value = get(testNs.name, event.getPartitionId());
            if (value != null) {
                values.put(testNs, value);
            }
        }

        return values.isEmpty() ? null : new TestFragmentReplicationOperation(values)
                .setServiceName(SERVICE_NAME);
    }

    @Override
    protected void onCommitMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            if (event.getNewReplicaIndex() == -1 || event.getNewReplicaIndex() > backupCount) {
                removePartitionData(event.getPartitionId());
            }
        }
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            if (event.getNewReplicaIndex() > backupCount) {
                for (Key key : data.keySet()) {
                    assertNotEquals(event.getPartitionId(), key.partitionId);
                }
            }
        }
    }

    @Override
    protected void onRollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            if (event.getCurrentReplicaIndex() == -1 || event.getCurrentReplicaIndex() > backupCount) {
                removePartitionData(event.getPartitionId());
            }
        }
    }

    private void removePartitionData(int partitionId) {
        Iterator<Key> iter = data.keySet().iterator();
        while (iter.hasNext()) {
            Key key = iter.next();
            if (key.partitionId == partitionId) {
                iter.remove();
            }
        }
    }

    @Override
    public int size(String name) {
        int k = 0;
        for (Key key : data.keySet()) {
            if (key.name.equals(name)) {
                k++;
            }
        }
        return k;
    }

    @Override
    public Integer get(String name, int id) {
        return data.get(new Key(name, id));
    }

    @Override
    public boolean contains(String name, int id) {
        return data.containsKey(new Key(name, id));
    }

    @Override
    public Collection<Integer> keys(String name) {
        Set<Integer> set = new HashSet<Integer>();
        for (Key key : data.keySet()) {
            if (key.name.equals(name)) {
                set.add(key.partitionId);
            }
        }
        return set;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public ServiceNamespace getNamespace(String name) {
        return new TestServiceNamespace(name);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return prepareReplicationOperation(event, getAllServiceNamespaces(event));
    }

    private static class Key {
        final String name;
        final int partitionId;

        Key(String name, int partitionId) {
            this.name = name;
            this.partitionId = partitionId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key key = (Key) o;
            return partitionId == key.partitionId && name.equals(key.name);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + partitionId;
            return result;
        }
    }
}
