/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap.proxy;

import com.hazelcast.config.cp.CPMapConfig;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftOp;
import com.hazelcast.cp.internal.RaftService;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapService;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapCompareAndSetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapDeleteOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapGetOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapOperationProvider;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutIfAbsentOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapPutOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapRemoveOp;
import com.hazelcast.cp.internal.datastructures.cpmap.operation.CPMapSetOp;
import com.hazelcast.cp.internal.datastructures.spi.operation.DestroyRaftObjectOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
/**
 * Server proxy implementation for {@link CPMap}.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
public class CPMapProxy<K, V> implements CPMap<K, V> {
    /**
     * Precondition exception message when CAS 'expectedValue' is {@code null}.
     */
    public static final String MESSAGE_EXPECTED_VALUE = "argument 'expectedValue' cannot be null";
    /**
     * Precondition exception message when CAS 'newValue' is {@code null}.
     */
    public static final String MESSAGE_NEW_VALUE = "argument 'newValue' cannot be null";
    /**
     * Precondition exception message when 'key' is {@code null}.
     */
    public static final String MESSAGE_KEY = "argument 'key' cannot be null";
    /**
     * Precondition exception message when 'value' is {@code null}.
     */
    public static final String MESSAGE_VALUE = "argument 'value' cannot be null";

    private final NodeEngine nodeEngine;
    private final CPGroupId groupId;
    private final String objectName;
    private final RaftInvocationManager invocationManager;
    private final String proxyName;
    private final SerializationService serializationService;
    private final CPMapOperationProvider cpMapOperationProvider;

    public CPMapProxy(NodeEngine nodeEngine, CPGroupId groupId, String proxyName) {
        this.nodeEngine = nodeEngine;
        this.groupId = groupId;
        this.proxyName = proxyName;

        RaftService raftService = nodeEngine.getService(RaftService.SERVICE_NAME);
        invocationManager = raftService.getInvocationManager();
        serializationService = nodeEngine.getSerializationService();
        // [objectName] is [proxyName] minus CP group qualification, e.g. proxyName=me@default -> objectName=me
        objectName = RaftService.getObjectNameForProxy(proxyName);
        CPMapConfig cpMapConfig = nodeEngine.getConfig()
                .getCPSubsystemConfig().findCPMapConfig(objectName);
        final boolean purgeEnabled = cpMapConfig != null && cpMapConfig.isPurgeEnabled();
        cpMapOperationProvider = ((CPMapService) nodeEngine.getService(CPMapService.SERVICE_NAME))
                .getCpMapOperationProvider(purgeEnabled);
    }

    private <T> T synchronousRaftOp(RaftOp raftOp, boolean isQuery) {
        InternalCompletableFuture<?> f =
                isQuery ? invocationManager.query(groupId, raftOp, QueryPolicy.LINEARIZABLE)
                        : invocationManager.invoke(groupId, raftOp);
        Object result = f.joinInternal();
        return serializationService.toObject(result);
    }

    private <T> T invoke(RaftOp raftOp) {
        return synchronousRaftOp(raftOp, false);
    }

    private <T> T query(RaftOp raftOp) {
        return synchronousRaftOp(raftOp, true);
    }

    private Data toData(Object o) {
        return serializationService.toData(o);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value) {
        return invokeKeyValueRaftOp(key, value, newCpMapPutOp(key, value));
    }

    protected CPMapPutOp newCpMapPutOp(K key, V value) {
        return cpMapOperationProvider.newCpMapPutOp(objectName, toData(key), toData(value));
    }

    @Override
    public V putIfAbsent(@Nonnull K key, @Nonnull V value) {
        return invokeKeyValueRaftOp(key, value, newCpMapPutIfAbsentOp(key, value));
    }

    protected CPMapPutIfAbsentOp newCpMapPutIfAbsentOp(K key, V value) {
        return cpMapOperationProvider.newCpMapPutIfAbsentOp(objectName, toData(key), toData(value));
    }

    @Override
    public void set(@Nonnull K key, @Nonnull V value) {
        invokeKeyValueRaftOp(key, value, newCpMapSetOp(key, value));
    }

    protected CPMapSetOp newCpMapSetOp(K key, V value) {
        return cpMapOperationProvider.newCpMapSetOp(objectName, toData(key), toData(value));
    }

    protected V invokeKeyValueRaftOp(K key, V value, RaftOp raftOp) {
        checkNotNull(key, MESSAGE_KEY);
        checkNotNull(value, MESSAGE_VALUE);
        return invoke(raftOp);
    }

    @Override
    public V remove(@Nonnull K key) {
        checkNotNull(key, MESSAGE_KEY);
        return invoke(new CPMapRemoveOp(objectName, toData(key)));
    }

    @Override
    public void delete(@Nonnull K key) {
        checkNotNull(key, MESSAGE_KEY);
        invoke(new CPMapDeleteOp(objectName, toData(key)));
    }

    @Override
    public boolean compareAndSet(@Nonnull K key, @Nonnull V expectedValue, @Nonnull V newValue) {
        checkNotNull(key, MESSAGE_KEY);
        checkNotNull(expectedValue, MESSAGE_EXPECTED_VALUE);
        checkNotNull(newValue, MESSAGE_NEW_VALUE);
        return invoke(newCompareAndSetOp(key, expectedValue, newValue));
    }

    protected CPMapCompareAndSetOp newCompareAndSetOp(K key, V expectedValue, V newValue) {
        return cpMapOperationProvider.newCompareAndSetOp(objectName, toData(key),
                toData(expectedValue), toData(newValue));
    }

    @Override
    public V get(@Nonnull K key) {
        checkNotNull(key, MESSAGE_KEY);
        return query(new CPMapGetOp(objectName, toData(key)));
    }

    @Override
    public String getPartitionKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return proxyName;
    }

    @Override
    public String getServiceName() {
        return CPMapService.SERVICE_NAME;
    }

    @Override
    public void destroy() {
        invocationManager.invoke(groupId, new DestroyRaftObjectOp(getServiceName(), objectName)).joinInternal();
    }

    public CPGroupId getGroupId() {
        return groupId;
    }
}
