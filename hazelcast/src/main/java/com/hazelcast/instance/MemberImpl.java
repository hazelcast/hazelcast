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

package com.hazelcast.instance;

import com.hazelcast.cluster.MemberAttributeOperationType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.operations.MemberAttributeChangedOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.function.Supplier;
import com.hazelcast.version.MemberVersion;

import java.util.Map;

import static com.hazelcast.cluster.MemberAttributeOperationType.PUT;
import static com.hazelcast.cluster.MemberAttributeOperationType.REMOVE;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.Collections.singletonMap;

@PrivateApi
public final class MemberImpl
        extends AbstractMember
        implements Member, HazelcastInstanceAware, IdentifiedDataSerializable {

    /**
     * Denotes that member list join version of a member is not known yet.
     */
    public static final int NA_MEMBER_LIST_JOIN_VERSION = -1;

    private boolean localMember;

    private volatile int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
    private volatile HazelcastInstanceImpl instance;
    private volatile ILogger logger;

    public MemberImpl() {
    }

    public MemberImpl(Address address, MemberVersion version, boolean localMember) {
        this(singletonMap(MEMBER, address), version, localMember, null, null, false, NA_MEMBER_LIST_JOIN_VERSION, null);
    }

    public MemberImpl(Address address, MemberVersion version, boolean localMember, String uuid) {
        this(singletonMap(MEMBER, address), version, localMember, uuid, null, false, NA_MEMBER_LIST_JOIN_VERSION, null);
    }

    public MemberImpl(MemberImpl member) {
        super(member);
        this.localMember = member.localMember;
        this.memberListJoinVersion = member.memberListJoinVersion;
        this.instance = member.instance;
    }

    private MemberImpl(Map<EndpointQualifier, Address> addresses, MemberVersion version, boolean localMember,
                       String uuid, Map<String, Object> attributes, boolean liteMember, int memberListJoinVersion,
                       HazelcastInstanceImpl instance) {
        super(addresses, version, uuid, attributes, liteMember);
        this.memberListJoinVersion = memberListJoinVersion;
        this.localMember = localMember;
        this.instance = instance;
    }

    @Override
    protected ILogger getLogger() {
        return logger;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastInstanceImpl) {
            instance = (HazelcastInstanceImpl) hazelcastInstance;
            localMember = instance.node.address.equals(address);
            logger = instance.node.getLogger(this.getClass().getName());
        }
    }

    @Override
    public boolean localMember() {
        return localMember;
    }

    @Override
    public String getStringAttribute(String key) {
        return (String) getAttribute(key);
    }

    @Override
    public void setStringAttribute(String key, String value) {
        setAttribute(key, value);
    }

    @Override
    public Boolean getBooleanAttribute(String key) {
        return (Boolean) getAttribute(key);
    }

    @Override
    public void setBooleanAttribute(String key, boolean value) {
        setAttribute(key, value);
    }

    @Override
    public Byte getByteAttribute(String key) {
        return (Byte) getAttribute(key);
    }

    @Override
    public void setByteAttribute(String key, byte value) {
        setAttribute(key, value);
    }

    @Override
    public Short getShortAttribute(String key) {
        return (Short) getAttribute(key);
    }

    @Override
    public void setShortAttribute(String key, short value) {
        setAttribute(key, value);
    }

    @Override
    public Integer getIntAttribute(String key) {
        return (Integer) getAttribute(key);
    }

    @Override
    public void setIntAttribute(String key, int value) {
        setAttribute(key, value);
    }

    @Override
    public Long getLongAttribute(String key) {
        return (Long) getAttribute(key);
    }

    @Override
    public void setLongAttribute(String key, long value) {
        setAttribute(key, value);
    }

    @Override
    public Float getFloatAttribute(String key) {
        return (Float) getAttribute(key);
    }

    @Override
    public void setFloatAttribute(String key, float value) {
        setAttribute(key, value);
    }

    @Override
    public Double getDoubleAttribute(String key) {
        return (Double) getAttribute(key);
    }

    @Override
    public void setDoubleAttribute(String key, double value) {
        setAttribute(key, value);
    }

    @Override
    public void removeAttribute(String key) {
        ensureLocalMember();
        isNotNull(key, "key");

        Object value = attributes.remove(key);
        if (value == null) {
            return;
        }

        if (instance != null) {
            invokeOnAllMembers(new MemberAttributeOperationSupplier(REMOVE, key, null));
        }
    }


    public void setMemberListJoinVersion(int memberListJoinVersion) {
        this.memberListJoinVersion = memberListJoinVersion;
    }

    public int getMemberListJoinVersion() {
        return memberListJoinVersion;
    }

    private void ensureLocalMember() {
        if (!localMember) {
            throw new UnsupportedOperationException("Attributes on remote members must not be changed");
        }
    }

    private void setAttribute(String key, Object value) {
        ensureLocalMember();
        isNotNull(key, "key");
        isNotNull(value, "value");

        Object oldValue = attributes.put(key, value);
        if (value.equals(oldValue)) {
            return;
        }

        if (instance != null) {
            invokeOnAllMembers(new MemberAttributeOperationSupplier(PUT, key, value));
        }
    }

    private void invokeOnAllMembers(Supplier<Operation> operationSupplier) {
        NodeEngineImpl nodeEngine = instance.node.nodeEngine;
        OperationService os = nodeEngine.getOperationService();
        try {
            for (Member member : nodeEngine.getClusterService().getMembers()) {
                if (!member.localMember()) {
                    os.invokeOnTarget(ClusterServiceImpl.SERVICE_NAME, operationSupplier.get(), member.getAddress());
                } else {
                    os.execute(operationSupplier.get());
                }
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }

    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return ClusterDataSerializerHook.MEMBER;
    }

    private class MemberAttributeOperationSupplier implements Supplier<Operation> {

        private final MemberAttributeOperationType operationType;
        private final String key;
        private final Object value;

        MemberAttributeOperationSupplier(MemberAttributeOperationType operationType, String key, Object value) {
            this.operationType = operationType;
            this.key = key;
            this.value = value;
        }

        @Override
        public Operation get() {
            NodeEngineImpl nodeEngine = instance.node.nodeEngine;
            String uuid = nodeEngine.getLocalMember().getUuid();
            return new MemberAttributeChangedOp(operationType, key, value)
                    .setCallerUuid(uuid).setNodeEngine(nodeEngine);
        }
    }

    public static class Builder {
        private final Map<EndpointQualifier, Address> addressMap;

        private Map<String, Object> attributes;
        private boolean localMember;
        private String uuid;
        private boolean liteMember;
        private MemberVersion version;
        private int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
        private HazelcastInstanceImpl instance;

        public Builder(Address address) {
            Preconditions.isNotNull(address, "address");
            this.addressMap = singletonMap(MEMBER, address);
        }

        public Builder(Map<EndpointQualifier, Address> addresses) {
            Preconditions.isNotNull(addresses, "addresses");
            Preconditions.isNotNull(addresses.get(MEMBER), "addresses.get(MEMBER)");
            this.addressMap = addresses;
        }

        public Builder localMember(boolean localMember) {
            this.localMember = localMember;
            return this;
        }

        public Builder version(MemberVersion memberVersion) {
            this.version = memberVersion;
            return this;
        }

        public Builder uuid(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder attributes(Map<String, Object> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder memberListJoinVersion(int memberListJoinVersion) {
            this.memberListJoinVersion = memberListJoinVersion;
            return this;
        }

        public Builder liteMember(boolean liteMember) {
            this.liteMember = liteMember;
            return this;
        }

        public Builder instance(HazelcastInstanceImpl hazelcastInstanceImpl) {
            this.instance = hazelcastInstanceImpl;
            return this;
        }

        public MemberImpl build() {
            return new MemberImpl(addressMap, version, localMember, uuid,
                    attributes, liteMember, memberListJoinVersion, instance);
        }
    }
}
