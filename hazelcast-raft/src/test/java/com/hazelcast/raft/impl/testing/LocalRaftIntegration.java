package com.hazelcast.raft.impl.testing;

import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingServiceImpl;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.SnapshotAwareService;
import com.hazelcast.raft.impl.RaftEndpoint;
import com.hazelcast.raft.impl.RaftIntegration;
import com.hazelcast.raft.impl.RaftNodeImpl;
import com.hazelcast.raft.impl.RaftRunnable;
import com.hazelcast.raft.impl.RaftUtil;
import com.hazelcast.raft.impl.dto.AppendFailureResponse;
import com.hazelcast.raft.impl.dto.AppendRequest;
import com.hazelcast.raft.impl.dto.AppendSuccessResponse;
import com.hazelcast.raft.impl.dto.InstallSnapshot;
import com.hazelcast.raft.impl.dto.PreVoteRequest;
import com.hazelcast.raft.impl.dto.PreVoteResponse;
import com.hazelcast.raft.impl.dto.VoteRequest;
import com.hazelcast.raft.impl.dto.VoteResponse;
import com.hazelcast.raft.impl.service.RestoreSnapshotRaftRunnable;
import com.hazelcast.raft.impl.util.SimpleCompletableFuture;
import com.hazelcast.version.MemberVersion;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
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

    private final RaftEndpoint localEndpoint;
    private final RaftGroupId raftGroupId;
    private final SnapshotAwareService service;
    private final ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    private final ConcurrentMap<RaftEndpoint, RaftNodeImpl> nodes = new ConcurrentHashMap<RaftEndpoint, RaftNodeImpl>();
    private final LoggingServiceImpl loggingService;

    private final Set<EndpointDropEntry> endpointDropRules = Collections.newSetFromMap(new ConcurrentHashMap<EndpointDropEntry, Boolean>());
    private final Set<Class> dropAllRules = Collections.newSetFromMap(new ConcurrentHashMap<Class, Boolean>());

    LocalRaftIntegration(TestRaftEndpoint localEndpoint, RaftGroupId raftGroupId, SnapshotAwareService service) {
        this.localEndpoint = localEndpoint;
        this.raftGroupId = raftGroupId;
        this.service = service;
        this.loggingService = new LoggingServiceImpl("dev", "log4j2", BuildInfoProvider.getBuildInfo());
        loggingService.setThisMember(getThisMember(localEndpoint));
    }

    private MemberImpl getThisMember(TestRaftEndpoint localEndpoint) {
        return new MemberImpl(RaftUtil.newAddress(localEndpoint.getPort()), MemberVersion.of(Versions.CURRENT_CLUSTER_VERSION.toString()), true, localEndpoint.getUid());
    }

    public void discoverNode(RaftNodeImpl node) {
        assertNotEquals(localEndpoint, node.getLocalEndpoint());
        RaftNodeImpl old = nodes.putIfAbsent(node.getLocalEndpoint(), node);
        assertThat(old, anyOf(nullValue(), sameInstance(node)));
    }

    public boolean removeNode(RaftNodeImpl node) {
        assertNotEquals(localEndpoint, node.getLocalEndpoint());
        return nodes.remove(node.getLocalEndpoint(), node);
    }

    public RaftEndpoint getLocalEndpoint() {
        return localEndpoint;
    }

    @Override
    public void execute(Runnable task) {
        scheduledExecutor.execute(task);
    }

    @Override
    public void schedule(Runnable task, long delay, TimeUnit timeUnit) {
        scheduledExecutor.schedule(task, delay, timeUnit);
    }

    @Override
    public SimpleCompletableFuture newCompletableFuture() {
        return new SimpleCompletableFuture(scheduledExecutor, loggingService.getLogger(getClass()));
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
    public boolean isReachable(RaftEndpoint endpoint) {
        return localEndpoint.equals(endpoint) || nodes.containsKey(endpoint);
    }

    @Override
    public boolean send(PreVoteRequest request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handlePreVoteRequest(request);
        return true;
    }

    @Override
    public boolean send(PreVoteResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handlePreVoteResponse(response);
        return true;
    }

    @Override
    public boolean send(VoteRequest request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleVoteRequest(request);
        return true;
    }

    @Override
    public boolean send(VoteResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleVoteResponse(response);
        return true;
    }

    @Override
    public boolean send(AppendRequest request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleAppendRequest(request);
        return true;
    }

    @Override
    public boolean send(AppendSuccessResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleAppendResponse(response);
        return true;
    }

    @Override
    public boolean send(AppendFailureResponse response, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(response, target)) {
            return true;
        }

        node.handleAppendResponse(response);
        return true;
    }

    @Override
    public boolean send(InstallSnapshot request, RaftEndpoint target) {
        assertNotEquals(localEndpoint, target);
        RaftNodeImpl node = nodes.get(target);
        if (node == null) {
            return false;
        }
        if (shouldDrop(request, target)) {
            return true;
        }

        node.handleInstallSnapshot(request);
        return true;
    }

    private boolean shouldDrop(Object message, RaftEndpoint target) {
        return dropAllRules.contains(message.getClass())
                || endpointDropRules.contains(new EndpointDropEntry(message.getClass(), target));
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
            Object snapshot = service.takeSnapshot(raftGroupId, commitIndex);
            return new RestoreSnapshotRaftRunnable(raftGroupId, commitIndex, snapshot);
        } catch (Throwable t) {
            return t;
        }
    }

    @Override
    public void restoreSnapshot(Object operation, long commitIndex) {
        runOperation(operation, commitIndex);
    }

    void dropMessagesToEndpoint(RaftEndpoint endpoint, Class messageType) {
        endpointDropRules.add(new EndpointDropEntry(messageType, endpoint));
    }

    void allowMessagesToEndpoint(RaftEndpoint endpoint, Class messageType) {
        endpointDropRules.remove(new EndpointDropEntry(messageType, endpoint));
    }

    void allowAllMessagesToEndpoint(RaftEndpoint endpoint) {
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

    void resetAllDropRules() {
        dropAllRules.clear();
        endpointDropRules.clear();
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
        final RaftEndpoint endpoint;

        private EndpointDropEntry(Class messageType, RaftEndpoint endpoint) {
            this.messageType = messageType;
            this.endpoint = endpoint;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof EndpointDropEntry)) return false;

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
}
