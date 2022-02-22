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

package com.hazelcast.splitbrainprotection.impl;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.config.SplitBrainProtectionListenerConfig;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.splitbrainprotection.HeartbeatAware;
import com.hazelcast.splitbrainprotection.PingAware;
import com.hazelcast.splitbrainprotection.SplitBrainProtection;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionEvent;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionFunction;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionListener;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.READ_WRITE;
import static com.hazelcast.splitbrainprotection.SplitBrainProtectionOn.WRITE;

/**
 * Service containing logic for cluster split brain protection.
 */
public class SplitBrainProtectionServiceImpl implements EventPublishingService<SplitBrainProtectionEvent,
        SplitBrainProtectionListener>, MembershipAwareService, SplitBrainProtectionService, HeartbeatAware, PingAware {

    public static final String SERVICE_NAME = "hz:impl:splitBrainProtectionService";

    /**
     * Single threaded split brain protection executor. Split brain protection
     * updates are executed in order, without concurrency.
     */
    private static final String SPLIT_BRAIN_PROTECTION_EXECUTOR = "hz:splitBrainProtection";

    private final NodeEngineImpl nodeEngine;
    private final EventService eventService;
    private volatile Map<String, SplitBrainProtectionImpl> splitBrainProtections;
    // true when at least one configured split brain protection implementation is HeartbeatAware
    private volatile boolean heartbeatAware;
    // true when at least one configured split brain protection implementation is PingAware
    private volatile boolean pingAware;

    public SplitBrainProtectionServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
    }

    public void start() {
        // before starting, no splitBrainProtections are used, just SplitBrainProtectionService dependency is
        // provided to services which depend on it so it's safe to initialize splitBrainProtections here (and we have
        // ClusterService already constructed)
        this.splitBrainProtections = Collections.unmodifiableMap(initializeSplitBrainProtections());
        scanSplitBrainProtections();
        initializeListeners();

        if (isInactive()) {
            return;
        }

        ExecutionService executionService = nodeEngine.getExecutionService();
        // single thread split brain protection executor
        executionService.register(SPLIT_BRAIN_PROTECTION_EXECUTOR, 1, Integer.MAX_VALUE, ExecutorType.CACHED);

        long heartbeatInterval = nodeEngine.getProperties().getSeconds(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS);
        executionService.scheduleWithRepetition(SPLIT_BRAIN_PROTECTION_EXECUTOR, new UpdateSplitBrainProtections(),
                heartbeatInterval, heartbeatInterval, TimeUnit.SECONDS);
    }

    private Map<String, SplitBrainProtectionImpl> initializeSplitBrainProtections() {
        Map<String, SplitBrainProtectionImpl> splitBrainProtections = new HashMap<>();
        for (SplitBrainProtectionConfig splitBrainProtectionConfig
                : nodeEngine.getConfig().getSplitBrainProtectionConfigs().values()) {
            validateSplitBrainProtectionConfig(splitBrainProtectionConfig);
            if (!splitBrainProtectionConfig.isEnabled()) {
                continue;
            }
            SplitBrainProtectionImpl splitBrainProtection = new SplitBrainProtectionImpl(splitBrainProtectionConfig, nodeEngine);
            splitBrainProtections.put(splitBrainProtectionConfig.getName(), splitBrainProtection);
        }
        return splitBrainProtections;
    }

    private void validateSplitBrainProtectionConfig(SplitBrainProtectionConfig splitBrainProtectionConfig) {
        if (splitBrainProtectionConfig.getFunctionImplementation() == null) {
            return;
        }

        SplitBrainProtectionFunction splitBrainProtectionFunction =
                splitBrainProtectionConfig.getFunctionImplementation();
        if (splitBrainProtectionFunction instanceof ProbabilisticSplitBrainProtectionFunction) {
            validateSplitBrainProtectionParameters(splitBrainProtectionConfig.getName(),
                    ((ProbabilisticSplitBrainProtectionFunction) splitBrainProtectionFunction).
                            getAcceptableHeartbeatPauseMillis(),
                    "acceptable heartbeat pause");
        } else if (splitBrainProtectionFunction instanceof RecentlyActiveSplitBrainProtectionFunction) {
            validateSplitBrainProtectionParameters(splitBrainProtectionConfig.getName(),
                    ((RecentlyActiveSplitBrainProtectionFunction) splitBrainProtectionFunction).
                            getHeartbeatToleranceMillis(),
                    "heartbeat tolerance");
        }
    }

    private void validateSplitBrainProtectionParameters(String splitBrainProtectionName, long value, String parameterName) {
        HazelcastProperties nodeProperties = nodeEngine.getProperties();
        long maxNoHeartbeatMillis = nodeProperties.getMillis(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS);
        long heartbeatIntervalMillis = nodeProperties.getMillis(ClusterProperty.HEARTBEAT_INTERVAL_SECONDS);

        if (value > maxNoHeartbeatMillis) {
            throw new InvalidConfigurationException("This member is configured with maximum no-heartbeat duration "
                + maxNoHeartbeatMillis + " millis. For the split brain protection '" + splitBrainProtectionName
                + "' to be effective, set " + parameterName + " to a lower value. Currently configured value is " + value
                + ", reconfigure to a value lower than " + maxNoHeartbeatMillis + ".");
        } else if (value < heartbeatIntervalMillis) {
            throw new InvalidConfigurationException("Split brain protection '" + splitBrainProtectionName
                    + "' is misconfigured: the value of " + "acceptable heartbeat pause (" + value
                    + ") must be greater than " + "the configured heartbeat interval (" + heartbeatIntervalMillis
                    + "), otherwise split brain protection " + "will be always absent.");
        }
    }

    private void initializeListeners() {
        for (Map.Entry<String, SplitBrainProtectionConfig> configEntry
                : nodeEngine.getConfig().getSplitBrainProtectionConfigs().entrySet()) {
            SplitBrainProtectionConfig config = configEntry.getValue();
            String instanceName = configEntry.getKey();
            for (SplitBrainProtectionListenerConfig listenerConfig : config.getListenerConfigs()) {
                initializeListenerInternal(instanceName, listenerConfig);
            }
        }
    }

    private void initializeListenerInternal(String instanceName, SplitBrainProtectionListenerConfig listenerConfig) {
        SplitBrainProtectionListener listener = null;
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
            addSplitBrainProtectionListener(instanceName, listener);
        }
    }

    // scan splitBrainProtections for heartbeat-aware and ping-aware implementations and set corresponding flags
    private void scanSplitBrainProtections() {
        for (SplitBrainProtectionImpl splitBrainProtection : splitBrainProtections.values()) {
            if (splitBrainProtection.isHeartbeatAware()) {
                this.heartbeatAware = true;
            }
            if (splitBrainProtection.isPingAware()) {
                this.pingAware = true;
            }
        }
    }

    private boolean isInactive() {
        return splitBrainProtections.isEmpty();
    }

    public void addSplitBrainProtectionListener(String name, SplitBrainProtectionListener listener) {
        eventService.registerLocalListener(SERVICE_NAME, name, listener);
    }

    /**
     * Ensures that the split brain protection for the given operation is present.
     * This requires that there is a split brain protection configured under the name which is returned by
     * the operation service. This in turn requires that the operation is named and that the operation service
     * is a {@link SplitBrainProtectionAwareService}.
     *
     * @param op the operation for which the minimum cluster size property should be satisfied
     * @throws SplitBrainProtectionException if the operation requires a split brain protection
     * and the minimum cluster size property is not satisfied
     */
    public void ensureNoSplitBrain(Operation op) {
        if (isInactive()) {
            return;
        }

        SplitBrainProtectionImpl splitBrainProtection = findSplitBrainProtection(op);
        if (splitBrainProtection == null) {
            return;
        }
        splitBrainProtection.ensureNoSplitBrain(op);
    }

    public void ensureNoSplitBrain(@Nullable String splitBrainProtectionName,
                                   @Nonnull SplitBrainProtectionOn requiredSplitBrainProtectionPermissionType) {
        checkNotNull(requiredSplitBrainProtectionPermissionType,
                "requiredSplitBrainProtectionPermissionType cannot be null!");
        if (isInactive() || splitBrainProtectionName == null) {
            return;
        }

        SplitBrainProtectionImpl definedSplitBrainProtection = splitBrainProtections.get(splitBrainProtectionName);
        if (definedSplitBrainProtection == null) {
            return;
        }
        SplitBrainProtectionOn definedSplitBrainProtectionOn = definedSplitBrainProtection.getConfig().getProtectOn();
        switch (requiredSplitBrainProtectionPermissionType) {
            case WRITE:
                if (definedSplitBrainProtectionOn.equals(WRITE) || definedSplitBrainProtectionOn.equals(READ_WRITE)) {
                    definedSplitBrainProtection.ensureNoSplitBrain();
                }
                break;
            case READ:
                if (definedSplitBrainProtectionOn.equals(READ) || definedSplitBrainProtectionOn.equals(READ_WRITE)) {
                    definedSplitBrainProtection.ensureNoSplitBrain();
                }
                break;
            case READ_WRITE:
                if (definedSplitBrainProtectionOn.equals(READ_WRITE)) {
                    definedSplitBrainProtection.ensureNoSplitBrain();
                }
                break;
            default:
                throw new IllegalStateException("Unhandled split brain protection type: "
                        + requiredSplitBrainProtectionPermissionType);
        }
    }

    /**
     * Returns a {@link SplitBrainProtectionImpl} for the given operation. The operation should be named and the
     * operation service should be a {@link SplitBrainProtectionAwareService}. This service will then return the
     * split brain protection configured under the name returned by the operation service.
     *
     * @param op the operation for which the SplitBrainProtection should be returned
     * @return the split brain protection
     */
    private SplitBrainProtectionImpl findSplitBrainProtection(Operation op) {
        if (!isNamedOperation(op)) {
            return null;
        }
        SplitBrainProtectionAwareService service = getSplitBrainProtectionAwareService(op);
        if (service == null) {
            return null;
        }
        String name = ((NamedOperation) op).getName();
        String splitBrainProtectionName = service.getSplitBrainProtectionName(name);
        if (splitBrainProtectionName == null) {
            return null;
        }
        return splitBrainProtections.get(splitBrainProtectionName);
    }

    private SplitBrainProtectionAwareService getSplitBrainProtectionAwareService(Operation op) {
        Object service;
        if (op instanceof ServiceNamespaceAware) {
            ServiceNamespace serviceNamespace = ((ServiceNamespaceAware) op).getServiceNamespace();
            service = nodeEngine.getService(serviceNamespace.getServiceName());
        } else {
            service = op.getService();
        }
        if (service instanceof SplitBrainProtectionAwareService) {
            return (SplitBrainProtectionAwareService) service;
        }
        return null;
    }

    private boolean isNamedOperation(Operation op) {
        return op instanceof NamedOperation;
    }

    @Override
    public void dispatchEvent(SplitBrainProtectionEvent event, SplitBrainProtectionListener listener) {
        listener.onChange(event);
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {
        if (isInactive() || event.getMember().localMember()) {
            return;
        }
        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_PROTECTION_EXECUTOR, new UpdateSplitBrainProtections(event));
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        if (isInactive()) {
            return;
        }
        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_PROTECTION_EXECUTOR, new UpdateSplitBrainProtections(event));
    }

    @Nonnull
    @Override
    public SplitBrainProtection getSplitBrainProtection(@Nonnull String splitBrainProtectionName) {
        checkNotNull(splitBrainProtectionName, "splitBrainProtectionName cannot be null!");
        SplitBrainProtection splitBrainProtection = splitBrainProtections.get(splitBrainProtectionName);
        if (splitBrainProtection == null) {
            throw new IllegalArgumentException("No split brain protection configuration named [ "
                    + splitBrainProtectionName + " ] is found!");
        }
        return splitBrainProtection;
    }

    @Override
    public void onHeartbeat(Member member, long timestamp) {
        if (isInactive() || !heartbeatAware) {
            return;
        }
        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_PROTECTION_EXECUTOR, new OnHeartbeat(member, timestamp));
    }

    @Override
    public void onPingLost(Member member) {
        if (isInactive() || !pingAware) {
            return;
        }
        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_PROTECTION_EXECUTOR, new OnPing(member, false));
    }

    @Override
    public void onPingRestored(Member member) {
        if (isInactive() || !pingAware) {
            return;
        }
        nodeEngine.getExecutionService().execute(SPLIT_BRAIN_PROTECTION_EXECUTOR, new OnPing(member, true));
    }

    private class UpdateSplitBrainProtections implements Runnable {
        private final MembershipEvent event;

        UpdateSplitBrainProtections() {
            this.event = null;
        }

        UpdateSplitBrainProtections(MembershipEvent event) {
            this.event = event;
        }

        @Override
        public void run() {
            ClusterService clusterService = nodeEngine.getClusterService();
            Collection<Member> members = clusterService.getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
            for (SplitBrainProtectionImpl splitBrainProtection : splitBrainProtections.values()) {
                if (event != null) {
                    switch (event.getEventType()) {
                        case MembershipEvent.MEMBER_ADDED:
                            splitBrainProtection.onMemberAdded(event);
                            break;
                        case MembershipEvent.MEMBER_REMOVED:
                            splitBrainProtection.onMemberRemoved(event);
                            break;
                        default:
                            // nop
                            break;
                    }
                }
                splitBrainProtection.update(members);
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
            for (SplitBrainProtectionImpl splitBrainProtection : splitBrainProtections.values()) {
                splitBrainProtection.onHeartbeat(member, timestamp);
                splitBrainProtection.update(members);
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
            for (SplitBrainProtectionImpl splitBrainProtection : splitBrainProtections.values()) {
                splitBrainProtection.onPing(member, successful);
                splitBrainProtection.update(members);
            }
        }
    }
}
