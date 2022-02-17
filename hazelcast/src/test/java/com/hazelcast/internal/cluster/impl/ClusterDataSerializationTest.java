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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.EndpointQualifier.REST;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterDataSerializationTest {

    private static final SerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();
    private static final ClusterStateChange<MemberVersion> VERSION_CLUSTER_STATE_CHANGE
            = ClusterStateChange.from(MemberVersion.of(
            BuildInfoProvider.getBuildInfo().getVersion()));
    private static final ClusterStateChange<ClusterState> CLUSTER_STATE_CHANGE = ClusterStateChange.from(ClusterState.FROZEN);

    @Test
    public void testSerializationOf_clusterStateChange_fromVersion() {
        Data serialized = SERIALIZATION_SERVICE.toData(VERSION_CLUSTER_STATE_CHANGE);
        ClusterStateChange deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(VERSION_CLUSTER_STATE_CHANGE, deserialized);
    }

    @Test
    public void testSerializationOf_clusterStateChange_fromClusterState() {
        Data serialized = SERIALIZATION_SERVICE.toData(CLUSTER_STATE_CHANGE);
        ClusterStateChange deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(CLUSTER_STATE_CHANGE, deserialized);
    }

    @Test
    public void testSerializationOf_clusterStateChangeTxnLogRecord_whenVersionChange() throws UnknownHostException {
        ClusterStateTransactionLogRecord txnLogRecord = new ClusterStateTransactionLogRecord(VERSION_CLUSTER_STATE_CHANGE,
                new Address("127.0.0.1", 5071), new Address("127.0.0.1", 5702), UUID.randomUUID(), 120,
                111, 130, false);

        Data serialized = SERIALIZATION_SERVICE.toData(txnLogRecord);

        ClusterStateTransactionLogRecord deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(txnLogRecord.stateChange, deserialized.stateChange);
        assertEquals(txnLogRecord.initiator, deserialized.initiator);
        assertEquals(txnLogRecord.target, deserialized.target);
        assertEquals(txnLogRecord.txnId, deserialized.txnId);
        assertEquals(txnLogRecord.leaseTime, deserialized.leaseTime);
        assertEquals(txnLogRecord.isTransient, deserialized.isTransient);
        assertEquals(txnLogRecord.memberListVersion, deserialized.memberListVersion);
        assertEquals(txnLogRecord.partitionStateStamp, deserialized.partitionStateStamp);
    }

    @Test
    public void testSerializationOf_clusterStateChangeTxnLogRecord_whenStateChange() throws UnknownHostException {
        ClusterStateTransactionLogRecord txnLogRecord = new ClusterStateTransactionLogRecord(CLUSTER_STATE_CHANGE,
                new Address("127.0.0.1", 5071), new Address("127.0.0.1", 5702), UUID.randomUUID(), 120,
                111, 130, false);

        Data serialized = SERIALIZATION_SERVICE.toData(txnLogRecord);

        ClusterStateTransactionLogRecord deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(txnLogRecord.stateChange, deserialized.stateChange);
        assertEquals(txnLogRecord.initiator, deserialized.initiator);
        assertEquals(txnLogRecord.target, deserialized.target);
        assertEquals(txnLogRecord.txnId, deserialized.txnId);
        assertEquals(txnLogRecord.leaseTime, deserialized.leaseTime);
        assertEquals(txnLogRecord.isTransient, deserialized.isTransient);
        assertEquals(txnLogRecord.memberListVersion, deserialized.memberListVersion);
        assertEquals(txnLogRecord.partitionStateStamp, deserialized.partitionStateStamp);
    }

    @Test
    public void testSerializationOf_memberInfo() throws UnknownHostException {
        Address memberAddress = new Address("127.0.0.1", 5071);
        Address clientAddress = new Address("127.0.0.1", 7654);
        Address restAddress = new Address("127.0.0.1", 8080);
        // member attributes, test an integer, a String and an IdentifiedDataSerializable as values
        Map<String, String> attributes = new HashMap<>();
        attributes.put("a", "2");
        attributes.put("b", "b");
        Map<EndpointQualifier, Address> addressMap = new HashMap<EndpointQualifier, Address>();
        addressMap.put(MEMBER, memberAddress);
        addressMap.put(CLIENT, clientAddress);
        addressMap.put(REST, restAddress);
        MemberInfo memberInfo = new MemberInfo(memberAddress, UUID.randomUUID(), attributes,
                false, MemberVersion.of(BuildInfoProvider.getBuildInfo().getVersion()), addressMap);

        Data serialized = SERIALIZATION_SERVICE.toData(memberInfo);

        MemberInfo deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(deserialized.getAddress(), memberInfo.getAddress());
        assertEquals(deserialized.getVersion(), memberInfo.getVersion());
        assertEquals(deserialized.getUuid(), memberInfo.getUuid());
        assertEquals(deserialized.getAttributes().get("a"), memberInfo.getAttributes().get("a"));
        assertEquals(deserialized.getAttributes().get("b"), memberInfo.getAttributes().get("b"));
        assertEquals(3, deserialized.getAddressMap().size());
        assertEquals(memberAddress, deserialized.getAddressMap().get(MEMBER));
        assertEquals(clientAddress, deserialized.getAddressMap().get(CLIENT));
        assertEquals(restAddress, deserialized.getAddressMap().get(REST));
    }
}
