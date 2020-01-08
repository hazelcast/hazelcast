/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.raft.impl.testing;

import com.hazelcast.core.Endpoint;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.raft.SnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.RaftNodeImpl;
import com.hazelcast.cp.internal.raft.impl.RaftNodeStatus;
import com.hazelcast.cp.internal.raft.impl.RaftUtil;
import com.hazelcast.cp.internal.raft.impl.dataservice.RestoreSnapshotRaftRunnable;
import com.hazelcast.cp.internal.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.cp.internal.raft.impl.dto.AppendRequest;
import com.hazelcast.cp.internal.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.cp.internal.raft.impl.dto.InstallSnapshot;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.PreVoteResponse;
import com.hazelcast.cp.internal.raft.impl.dto.VoteRequest;
import com.hazelcast.cp.internal.raft.impl.dto.VoteResponse;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.util.function.Function;
import com.hazelcast.version.MemberVersion;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

/**
 * In-memory {@link RaftIntegration} implementation for Raft core testing. Creates a single thread executor
 * to execute/schedule tasks and operations.
 * <p>
 * Additionally provides a mechanism to define custom drop/allow rules for specific message types and endpoints.
 */
public class LocalRaftIntegration implements RaftIntegration {

    private final Endpoint localEndpoint;
    private final CPGroupId groupId;
    private final SnapshotAwareService service;
    private final boolean appendNopEntryOnLeaderElection;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<Endpoint, RaftNodeImpl> nodes = new ConcurrentHashMap<Endpoint, RaftNodeImpl>();
    private final LoggingServiceImpl loggingService;

    private final Set<EndpointDropEntry> endpointDropRules = Collections.newSetFromMap(new ConcurrentHashMap<EndpointDropEntry, Boolean>());
    private final Map<Endpoint, Function<Object, Object>> alterRPCRules = new ConcurrentHashMap<Endpoint, Function<Object, Object>>();
    private final Set<Class> dropAllRules = Collections.newSetFromMap(new ConcurrentHashMap<Class, Boolean>());

    LocalRaftIntegration(TestRaftMember localEndpoint, CPGroupId groupId, SnapshotAwareService service,
                         boolean appendNopEntryOnLeaderElection) {
        this.localEndpoint = localEndpoint;
        this.groupId = groupId;
        this.service = service;
        this.appendNopEntryOnLeaderElection = appendNopEntryOnLeaderElection;
        this.loggingService = new LoggingServiceImpl("dev", "log4j2", BuildInfoProvider.getBuildInfo());
        loggingService.setThisMember(getThisMember(localEndpoint));
    }

    private MemberImpl getThisMember(TestRaftMember localEndpoint) {
        return new MemberImpl(RaftUtil.newAddress(localEndpoint.getPort()), MemberVersion.of(Versions.CURRENT_CLUSTER_VERSION.toString()), true, localEndpoint.getUuid());
    }

    public void discoverNode(RaftNodeImpl node) {
        assertNotEquals(localEndpoint, node.getLocalMember());
        RaftNodeImpl old = nodes.putIfAbsent(node.getLocalMember(), node);
        assertThat(old, anyOf(nullValue(), sameInstance(node)));
    }

    public boolean removeNode(RaftNodeImpl node) {
        assertNotEquals(localEndpoint, node.getLocalMember());
        return nodes.remove(node.getLocalMember(), node);
    }

    public Endpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Override
    public void execute(Runnable task) {
        try {
            scheduledExecutor.execute(task);
        } catch (RejectedExecutionException e) {
            loggingService.getLogger(getClass()).fine(e);
        }
    }

    @Override
    public void schedule(Runnable task, long delay, TimeUnit timeUnit) {
        try {
            scheduledExecutor.schedule(task, delay, timeUnit);
        } catch (RejectedExecutionException e) {
            loggingService.getLogger(getClass()).fine(e);
        }
    }

    @Override
    public SimpleCompletableFuture newCompletableFuture() {
        return new SimpleCompletableFuture(scheduledExecutor, loggingService.getLogger(getClass()));
    }

    @Override
    public Object getAppendedEntryOnLeaderElection() {
        return appendNopEntryOnLeaderElection ? new NopEntry() : null;
    }

    @Override
    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public boolean isReachable(Endpoint endpoint) {
        return localEndpoint.equals(endpoint) || nodes.containsKey(endpoint);
    }

    @Override
    public boolean send(PreVoteRequest request, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handlePreVoteRequest(alterMessageIfNeeded(request, target));
        return true;
    }

    @Override
    public boolean send(PreVoteResponse response, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handlePreVoteResponse(alterMessageIfNeeded(response, target));
        return true;
    }

    @Override
    public boolean send(VoteRequest request, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleVoteRequest(alterMessageIfNeeded(request, target));
        return true;
    }

    @Override
    public boolean send(VoteResponse response, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleVoteResponse(alterMessageIfNeeded(response, target));
        return true;
    }

    @Override
    public boolean send(AppendRequest request, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleAppendRequest(alterMessageIfNeeded(request, target));
        return true;
    }

    @Override
    public boolean send(AppendSuccessResponse response, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleAppendResponse(alterMessageIfNeeded(response, target));
        return true;
    }

    @Override
    public boolean send(AppendFailureResponse response, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleAppendResponse(alterMessageIfNeeded(response, target));
        return true;
    }

    @Override
    public boolean send(InstallSnapshot request, Endpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleInstallSnapshot(alterMessageIfNeeded(request, target));
        return true;
    }

    private boolean shouldDrop(Object message, Endpoint target) {
        return dropAllRules.contains(message.getClass())
                || endpointDropRules.contains(new EndpointDropEntry(message.getClass(), target));
    }

    private <T> T alterMessageIfNeeded(T message, Endpoint endpoint) {
        Function<Object, Object> alterFunc = alterRPCRules.get(endpoint);
        if (alterFunc != null) {
            Object alteredMessage = alterFunc.apply(message);
            if (alteredMessage != null) {
                return (T) alteredMessage;
            }
        }

        return message;
    }

    @Override
    public Object runOperation(Object op, long commitIndex) {
        if (op == null) {
            return null;
        }

        try {
            RaftRunnable operation = (RaftRunnable) op;
            return operation.run(service, commitIndex);
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public Object takeSnapshot(long commitIndex) {
        try {
            Object snapshot = service.takeSnapshot(groupId, commitIndex);
            return new RestoreSnapshotRaftRunnable(groupId, commitIndex, snapshot);
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public void restoreSnapshot(Object operation, long commitIndex) {
        runOperation(operation, commitIndex);
    }

    void dropMessagesToEndpoint(Endpoint endpoint, Class messageType) {
        endpointDropRules.add(new EndpointDropEntry(messageType, endpoint));
    }

    void allowMessagesToEndpoint(Endpoint endpoint, Class messageType) {
        endpointDropRules.remove(new EndpointDropEntry(messageType, endpoint));
    }

    void allowAllMessagesToEndpoint(Endpoint endpoint) {
        Iterator<EndpointDropEntry> iter = endpointDropRules.iterator();
        while (iter.hasNext()) {
            EndpointDropEntry entry = iter.next();
            if (endpoint.equals(entry.endpoint)) {
                iter.remove();
            }
        }
    }

    void dropMessagesToAll(Class messageType) {
        dropAllRules.add(messageType);
    }

    void allowMessagesToAll(Class messageType) {
        dropAllRules.remove(messageType);
    }

    void resetAllRules() {
        dropAllRules.clear();
        endpointDropRules.clear();
        alterRPCRules.clear();
    }

    void alterMessagesToEndpoint(Endpoint endpoint, Function<Object, Object> function) {
        alterRPCRules.put(endpoint, function);
    }

    void removeAlterMessageRuleToEndpoint(Endpoint endpoint) {
        alterRPCRules.remove(endpoint);
    }

    public <T extends SnapshotAwareService> T getService() {
        return (T) service;
    }

    void shutdown() {
        scheduledExecutor.shutdown();
    }

    boolean isShutdown() {
        return scheduledExecutor.isShutdown();
    }

    private static class EndpointDropEntry {
        final Class messageType;
        final Endpoint endpoint;

        private EndpointDropEntry(Class messageType, Endpoint endpoint) {
            this.messageType = messageType;
            this.endpoint = endpoint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointDropEntry)) {
                return false;
            }

            EndpointDropEntry that = (EndpointDropEntry) o;
            return messageType.equals(that.messageType) && endpoint.equals(that.endpoint);
        }

        @Override
        public int hashCode() {
            int result = messageType.hashCode();
            result = 31 * result + endpoint.hashCode();
            return result;
        }
    }

    @Override
    public void onNodeStatusChange(RaftNodeStatus status) {
    }
}
