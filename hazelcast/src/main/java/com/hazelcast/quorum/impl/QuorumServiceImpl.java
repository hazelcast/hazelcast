/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.quorum.impl;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.ConfigurationException;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.QuorumListenerConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.HeartbeatAware;
import com.hazelcast.quorum.PingAware;
import com.hazelcast.quorum.Quorum;
import com.hazelcast.quorum.QuorumEvent;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.quorum.QuorumListener;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.ServiceNamespace;
import com.hazelcast.spi.ServiceNamespaceAware;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ExecutorType;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.quorum.QuorumType.READ;
import static com.hazelcast.quorum.QuorumType.READ_WRITE;
import static com.hazelcast.quorum.QuorumType.WRITE;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Service containing logic for cluster quorum.
 *
 * IMPORTANT: The term "quorum" simply refers to the count of members in the cluster required for an operation to succeed.
 * It does NOT refer to an implementation of Paxos or Raft protocols as used in many NoSQL and distributed systems.
 * The mechanism it provides in Hazelcast protects the user in case the number of nodes in a cluster drops below the
 * specified one.
 */
public class QuorumServiceImpl implements EventPublishingService<QuorumEvent, QuorumListener>, MembershipAwareService,
                                          QuorumService, HeartbeatAware, PingAware {

    public static final String SERVICE_NAME = "hz:impl:quorumService";

    /**
     * Single threaded quorum executor. Quorum updates are executed in order, without concurrency.
     */
    private static final String QUORUM_EXECUTOR = "hz:quorum";

    private final NodeEngineImpl nodeEngine;
    private final EventService eventService;
    private volatile Map<String, QuorumImpl> quorums;
    // true when at least one configured quorum implementation is HeartbeatAware
    private volatile boolean heartbeatAware;
    // true when at least one configured quorum implementation is PingAware
    private volatile boolean pingAware;

    public QuorumServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
    }

    public void start() {
        // before starting, no quorums are used, just QuorumService dependency is provided to services which depend on it
        // so it's safe to initialize quorums here (and we have ClusterService already constructed)
        this.quorums = Collections.unmodifiableMap(initializeQuorums());
        scanQuorums();
        initializeListeners();

        if (isInactive()) {
            return;
        }

        InternalExecutionService executionService = nodeEngine.getExecutionService();
        // single thread quorum executor
        executionService.register(QUORUM_EXECUTOR, 1, Integer.MAX_VALUE, ExecutorType.CACHED);

        long heartbeatInterval = nodeEngine.getProperties().getSeconds(GroupProperty.HEARTBEAT_INTERVAL_SECONDS);
        executionService.scheduleWithRepetition(QUORUM_EXECUTOR, new UpdateQuorums(),
                heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
    }

    private Map<String, QuorumImpl> initializeQuorums() {
        Map<String, QuorumImpl> quorums = new HashMap<String, QuorumImpl>();
        for (QuorumConfig quorumConfig : nodeEngine.getConfig().getQuorumConfigs().values()) {
            validateQuorumConfig(quorumConfig);
            QuorumImpl quorum = new QuorumImpl(quorumConfig, nodeEngine);
            quorums.put(quorumConfig.getName(), quorum);
        }
        return quorums;
    }

    private void validateQuorumConfig(QuorumConfig quorumConfig) {
        if (quorumConfig.getQuorumFunctionImplementation() == null) {
            return;
        }

        QuorumFunction quorumFunction = quorumConfig.getQuorumFunctionImplementation();
        if (quorumFunction instanceof ProbabilisticQuorumFunction) {
            validateQuorumParameters(quorumConfig.getName(),
                    ((ProbabilisticQuorumFunction) quorumFunction).getAcceptableHeartbeatPauseMillis(),
                    "acceptable heartbeat pause");
        } else if (quorumFunction instanceof RecentlyActiveQuorumFunction) {
            validateQuorumParameters(quorumConfig.getName(),
                    ((RecentlyActiveQuorumFunction) quorumFunction).getHeartbeatToleranceMillis(),
                    "heartbeat tolerance");
        }
    }

    private void validateQuorumParameters(String quorumName, long value, String parameterName) {
        HazelcastProperties nodeProperties = nodeEngine.getProperties();
        long maxNoHeartbeatMillis = nodeProperties.getMillis(GroupProperty.MAX_NO_HEARTBEAT_SECONDS);
        long heartbeatIntervalMillis = nodeProperties.getMillis(GroupProperty.HEARTBEAT_INTERVAL_SECONDS);

        if (value > maxNoHeartbeatMillis) {
            throw new ConfigurationException("This member is configured with maximum no-heartbeat duration "
                + maxNoHeartbeatMillis + " millis. For the quorum '" + quorumName + "' to be effective, set "
                + parameterName + " to a lower value. Currently configured value is " + value
                + ", reconfigure to a value lower than " + maxNoHeartbeatMillis + ".");
        } else if (value < heartbeatIntervalMillis) {
            throw new ConfigurationException("Quorum '" + quorumName + "' is misconfigured: the value of "
                    + "acceptable heartbeat pause (" + value + ") must be greater than "
                    + "the configured heartbeat interval (" + heartbeatIntervalMillis + "), otherwise quorum "
                    + "will be always absent.");
        }
    }

    private void initializeListeners() {
        for (Map.Entry<String, QuorumConfig> configEntry : nodeEngine.getConfig().getQuorumConfigs().entrySet()) {
            QuorumConfig config = configEntry.getValue();
            String instanceName = configEntry.getKey();
            for (QuorumListenerConfig listenerConfig : config.getListenerConfigs()) {
                initializeListenerInternal(instanceName, listenerConfig);
            }
        }
    }

    private void initializeListenerInternal(String instanceName, QuorumListenerConfig listenerConfig) {
        QuorumListener listener = null;
        if (listenerConfig.getImplementation() != null) {
            listener = listenerConfig.getImplementation();
        } else if (listenerConfig.getClassName() != null) {
            try {
                listener = ClassLoaderUtil
                        .newInstance(nodeEngine.getConfigClassLoader(), listenerConfig.getClassName());
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (listener != null) {
            addQuorumListener(instanceName, listener);
        }
    }

    // scan quorums for heartbeat-aware and ping-aware implementations and set corresponding flags
    private void scanQuorums() {
        for (QuorumImpl quorum : quorums.values()) {
            if (quorum.isHeartbeatAware()) {
                this.heartbeatAware = true;
            }
            if (quorum.isPingAware()) {
                this.pingAware = true;
            }
        }
    }

    private boolean isInactive() {
        return quorums.isEmpty();
    }

    public void addQuorumListener(String name, QuorumListener listener) {
        eventService.registerLocalListener(SERVICE_NAME, name, listener);
    }

    /**
     * Ensures that the quorum for the given operation is present. This requires that there is a quorum configured under the
     * name which is returned by the operation service.
     * This in turn requires that the operation is named and that the operation service is a {@link QuorumAwareService}.
     *
     * @param op the operation for which the quorum should be present
     * @throws QuorumException if the operation requires a quorum and the quorum is not present
     */
    public void ensureQuorumPresent(Operation op) {
        if (isInactive()) {
            return;
        }

        QuorumImpl quorum = findQuorum(op);
        if (quorum == null) {
            return;
        }
        quorum.ensureQuorumPresent(op);
    }

    public void ensureQuorumPresent(String quorumName, QuorumType requiredQuorumPermissionType) {
        if (isInactive() || quorumName == null) {
            return;
        }

        QuorumImpl definedQuorum = quorums.get(quorumName);
        QuorumType definedQuorumType = definedQuorum.getConfig().getType();
        switch (requiredQuorumPermissionType) {
            case WRITE:
                if (definedQuorumType.equals(WRITE) || definedQuorumType.equals(READ_WRITE)) {
                    definedQuorum.ensureQuorumPresent();
                }
                break;
            case READ:
                if (definedQuorumType.equals(READ) || definedQuorumType.equals(READ_WRITE)) {
                    definedQuorum.ensureQuorumPresent();
                }
                break;
            case READ_WRITE:
                if (definedQuorumType.equals(READ_WRITE)) {
                    definedQuorum.ensureQuorumPresent();
                }
                break;
            default:
                throw new IllegalStateException("Unhandled quorum type: " + requiredQuorumPermissionType);
        }
    }

    /**
     * Returns a {@link QuorumImpl} for the given operation. The operation should be named and the operation service should
     * be a {@link QuorumAwareService}. This service will then return the quorum configured under the name returned by the
     * operation service.
     *
     * @param op the operation for which the Quorum should be returned
     * @return the quorum
     */
    private QuorumImpl findQuorum(Operation op) {
        if (!isNamedOperation(op) || !isQuorumAware(op)) {
            return null;
        }
        String quorumName = getQuorumName(op);
        if (quorumName == null) {
            return null;
        }
        return quorums.get(quorumName);
    }

    private String getQuorumName(Operation op) {
        QuorumAwareService service;
        if (op instanceof ServiceNamespaceAware) {
            ServiceNamespace serviceNamespace = ((ServiceNamespaceAware) op).getServiceNamespace();
            service = nodeEngine.getService(serviceNamespace.getServiceName());
        } else {
            service = op.getService();
        }
        String name = ((NamedOperation) op).getName();
        return service.getQuorumName(name);
    }

    private boolean isQuorumAware(Operation op) {
        return op.getService() instanceof QuorumAwareService;
    }

    private boolean isNamedOperation(Operation op) {
        return op instanceof NamedOperation;
    }

    @Override
    public void dispatchEvent(QuorumEvent event, QuorumListener listener) {
        listener.onChange(event);
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        if (isInactive()) {
            return;
        }
        nodeEngine.getExecutionService().execute(QUORUM_EXECUTOR, new UpdateQuorums(event));
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        if (isInactive()) {
            return;
        }
        nodeEngine.getExecutionService().execute(QUORUM_EXECUTOR, new UpdateQuorums(event));
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
        // nop
        // MemberAttributeServiceEvent does NOT contain set of members
        // They cannot change quorum state
    }

    @Override
    public Quorum getQuorum(String quorumName) {
        checkNotNull(quorumName, "quorumName cannot be null!");
        Quorum quorum = quorums.get(quorumName);
        if (quorum == null) {
            throw new IllegalArgumentException("No quorum configuration named [ " + quorumName + " ] is found!");
        }
        return quorum;
    }

    @Override
    public void onHeartbeat(Member member, long timestamp) {
        if (isInactive() || !heartbeatAware) {
            return;
        }
        nodeEngine.getExecutionService().execute(QUORUM_EXECUTOR, new OnHeartbeat(member, timestamp));
    }

    @Override
    public void onPingLost(Member member) {
        if (isInactive() || !pingAware) {
            return;
        }
        nodeEngine.getExecutionService().execute(QUORUM_EXECUTOR, new OnPing(member, false));
    }

    @Override
    public void onPingRestored(Member member) {
        if (isInactive() || !pingAware) {
            return;
        }
        nodeEngine.getExecutionService().execute(QUORUM_EXECUTOR, new OnPing(member, true));
    }

    private class UpdateQuorums implements Runnable {
        private final MembershipEvent event;

        UpdateQuorums() {
            this.event = null;
        }

        UpdateQuorums(MembershipEvent event) {
            this.event = event;
        }

        @Override
        public void run() {
            ClusterService clusterService = nodeEngine.getClusterService();
            Collection<Member> members = clusterService.getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
            for (QuorumImpl quorum : quorums.values()) {
                if (event != null) {
                    switch (event.getEventType()) {
                        case MembershipEvent.MEMBER_ADDED:
                            quorum.onMemberAdded(event);
                            break;
                        case MembershipEvent.MEMBER_REMOVED:
                            quorum.onMemberRemoved(event);
                            break;
                        default:
                            // nop
                            break;
                    }
                }
                quorum.update(members);
            }
        }
    }

    private class OnHeartbeat implements Runnable {
        private final Member member;
        private final long timestamp;

        OnHeartbeat(Member member, long timestamp) {
            this.member = member;
            this.timestamp = timestamp;
        }

        @Override
        public void run() {
            ClusterService clusterService = nodeEngine.getClusterService();
            Collection<Member> members = clusterService.getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
            for (QuorumImpl quorum : quorums.values()) {
                quorum.onHeartbeat(member, timestamp);
                quorum.update(members);
            }
        }
    }

    private class OnPing implements Runnable {
        private final Member member;
        private final boolean successful;

        OnPing(Member member, boolean successful) {
            this.member = member;
            this.successful = successful;
        }

        @Override
        public void run() {
            ClusterService clusterService = nodeEngine.getClusterService();
            Collection<Member> members = clusterService.getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
            for (QuorumImpl quorum : quorums.values()) {
                quorum.onPing(member, successful);
                quorum.update(members);
            }
        }
    }
}
