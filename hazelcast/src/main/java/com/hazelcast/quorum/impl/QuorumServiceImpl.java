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

package com.hazelcast.quorum.impl;

import com.hazelcast.cluster.impl.MemberSelectingCollection;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.QuorumListenerConfig;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.quorum.Quorum;
import com.hazelcast.quorum.QuorumEvent;
import com.hazelcast.quorum.QuorumListener;
import com.hazelcast.quorum.QuorumService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.MemberAttributeServiceEvent;
import com.hazelcast.spi.MembershipAwareService;
import com.hazelcast.spi.MembershipServiceEvent;
import com.hazelcast.spi.NamedOperation;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Service containing logic for cluster quorum.
 */
public class QuorumServiceImpl implements EventPublishingService<QuorumEvent, QuorumListener>, MembershipAwareService,
        QuorumService {

    public static final String SERVICE_NAME = "hz:impl:quorumService";

    private final NodeEngineImpl nodeEngine;
    private final EventService eventService;
    private boolean inactive;
    private final Map<String, QuorumImpl> quorums = new HashMap<String, QuorumImpl>();

    public QuorumServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.eventService = nodeEngine.getEventService();
        initializeQuorums();
        this.inactive = quorums.isEmpty();
    }

    public void start() {
        initializeListeners();
    }

    private void initializeQuorums() {
        for (QuorumConfig quorumConfig : nodeEngine.getConfig().getQuorumConfigs().values()) {
            QuorumImpl quorum = new QuorumImpl(quorumConfig, nodeEngine);
            quorums.put(quorumConfig.getName(), quorum);
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

    public void addQuorumListener(String name, QuorumListener listener) {
        eventService.registerLocalListener(SERVICE_NAME, name, listener);
    }

    public void ensureQuorumPresent(Operation op) {
        if (inactive) {
            return;
        }

        QuorumImpl quorum = findQuorum(op);
        if (quorum == null) {
            return;
        }
        quorum.ensureQuorumPresent(op);
    }

    private QuorumImpl findQuorum(Operation op) {
        if (!isNamedOperation(op) || !isQuorumAware(op)) {
            return null;
        }
        QuorumAwareService service = op.getService();
        String name = ((NamedOperation) op).getName();
        String quorumName = service.getQuorumName(name);
        if (quorumName == null) {
            return null;
        }
        return quorums.get(quorumName);
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
        updateQuorums(event);
    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        updateQuorums(event);
    }

    @Override
    public void memberAttributeChanged(MemberAttributeServiceEvent event) {
        // nop
        // MemberAttributeServiceEvent does NOT contain set of members
        // They cannot change quorum state
    }

    private void updateQuorums(MembershipEvent event) {
        final Collection<Member> members = new MemberSelectingCollection<Member>(event.getMembers(), DATA_MEMBER_SELECTOR);
        for (QuorumImpl quorum : quorums.values()) {
            quorum.update(members);
        }
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
}
