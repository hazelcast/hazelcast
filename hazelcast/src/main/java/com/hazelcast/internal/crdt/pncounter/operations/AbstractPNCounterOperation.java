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

package com.hazelcast.internal.crdt.pncounter.operations;

import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.cluster.Member;
import com.hazelcast.crdt.pncounter.PNCounter;
import com.hazelcast.internal.crdt.CRDTDataSerializerHook;
import com.hazelcast.crdt.TargetNotReplicaException;
import com.hazelcast.internal.crdt.pncounter.PNCounterImpl;
import com.hazelcast.internal.crdt.pncounter.PNCounterService;
import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.NamedOperation;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static com.hazelcast.internal.crdt.pncounter.PNCounterService.SERVICE_NAME;

/**
 * Base class for {@link PNCounter} query and
 * mutation operation implementations. It will throw an exception on all
 * serialization and deserialization invocations as CRDT operations must
 * be invoked locally on a member.
 */
public abstract class AbstractPNCounterOperation extends Operation implements IdentifiedDataSerializable, NamedOperation {
    protected String name;
    private PNCounterImpl counter;

    AbstractPNCounterOperation() {
    }

    AbstractPNCounterOperation(String name) {
        this.name = name;
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    PNCounterImpl getPNCounter(VectorClock observedTimestamps) {
        if (counter != null) {
            return counter;
        }
        final PNCounterService service = getService();
        if (observedTimestamps != null && !observedTimestamps.isEmpty() && !service.containsCounter(name)) {
            throw new ConsistencyLostException("This replica cannot provide the session guarantees for "
                    + "the PN counter since it's state is stale");
        }
        final int maxConfiguredReplicaCount = getNodeEngine().getConfig().findPNCounterConfig(name).getReplicaCount();
        if (!isCRDTReplica(maxConfiguredReplicaCount)) {
            throw new TargetNotReplicaException("This member is not a CRDT replica for the " + name + " + PN counter");
        }
        this.counter = service.getCounter(name);
        return counter;
    }

    /**
     * Returns {@code true} if this member is a CRDT replica.
     *
     * @param configuredReplicaCount the configured max replica count
     */
    private boolean isCRDTReplica(int configuredReplicaCount) {
        final Collection<Member> dataMembers = getNodeEngine().getClusterService()
                                                              .getMembers(MemberSelectors.DATA_MEMBER_SELECTOR);
        final Iterator<Member> dataMemberIterator = dataMembers.iterator();
        final Address thisAddress = getNodeEngine().getThisAddress();

        for (int i = 0; i < Math.min(configuredReplicaCount, dataMembers.size()); i++) {
            final Address dataMemberAddress = dataMemberIterator.next().getAddress();
            if (thisAddress.equals(dataMemberAddress)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    protected void toString(StringBuilder sb) {
        super.toString(sb);
        sb.append(", name=").append(name);
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeString(name);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        name = in.readString();
    }
}
