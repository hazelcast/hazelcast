/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.instance.BuildInfoProvider.BUILD_INFO;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClusterDataSerializationTest {

    private static final SerializationService SERIALIZATION_SERVICE = new DefaultSerializationServiceBuilder().build();
    private static final ClusterStateChange<MemberVersion> VERSION_CLUSTER_STATE_CHANGE = ClusterStateChange.from(MemberVersion.of(
            BUILD_INFO.getVersion()));
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
                new Address("127.0.0.1", 5071), new Address("127.0.0.1", 5702), UUID.randomUUID().toString(), 120, 130, false);

        Data serialized = SERIALIZATION_SERVICE.toData(txnLogRecord);

        ClusterStateTransactionLogRecord deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(txnLogRecord.stateChange, deserialized.stateChange);
        assertEquals(txnLogRecord.initiator, deserialized.initiator);
        assertEquals(txnLogRecord.target, deserialized.target);
        assertEquals(txnLogRecord.txnId, deserialized.txnId);
        assertEquals(txnLogRecord.leaseTime, deserialized.leaseTime);
        assertEquals(txnLogRecord.isTransient, deserialized.isTransient);
        assertEquals(txnLogRecord.partitionStateVersion, deserialized.partitionStateVersion);
    }

    @Test
    public void testSerializationOf_clusterStateChangeTxnLogRecord_whenStateChange() throws UnknownHostException {
        ClusterStateTransactionLogRecord txnLogRecord = new ClusterStateTransactionLogRecord(CLUSTER_STATE_CHANGE,
                new Address("127.0.0.1", 5071), new Address("127.0.0.1", 5702), UUID.randomUUID().toString(), 120, 130, false);

        Data serialized = SERIALIZATION_SERVICE.toData(txnLogRecord);

        ClusterStateTransactionLogRecord deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(txnLogRecord.stateChange, deserialized.stateChange);
        assertEquals(txnLogRecord.initiator, deserialized.initiator);
        assertEquals(txnLogRecord.target, deserialized.target);
        assertEquals(txnLogRecord.txnId, deserialized.txnId);
        assertEquals(txnLogRecord.leaseTime, deserialized.leaseTime);
        assertEquals(txnLogRecord.isTransient, deserialized.isTransient);
        assertEquals(txnLogRecord.partitionStateVersion, deserialized.partitionStateVersion);
    }

    @Test
    public void testSerializationOf_memberInfo() throws UnknownHostException {
        // member attributes, test an integer, a String and an IdentifiedDataSerializable as values
        Map<String, Object> attributes = new HashMap<String, Object>();
        attributes.put("a", 2);
        attributes.put("b", "b");
        attributes.put("c", new Address("127.0.0.1", 5999));
        MemberInfo memberInfo = new MemberInfo(new Address("127.0.0.1", 5071), UUID.randomUUID().toString(), attributes,
                false, MemberVersion.of(BUILD_INFO.getVersion()));

        Data serialized = SERIALIZATION_SERVICE.toData(memberInfo);

        MemberInfo deserialized = SERIALIZATION_SERVICE.toObject(serialized);
        assertEquals(deserialized.getAddress(), memberInfo.getAddress());
        assertEquals(deserialized.getVersion(), memberInfo.getVersion());
        assertEquals(deserialized.getUuid(), memberInfo.getUuid());
        assertEquals(deserialized.getAttributes().get("a"), memberInfo.getAttributes().get("a"));
        assertEquals(deserialized.getAttributes().get("b"), memberInfo.getAttributes().get("b"));
        assertEquals(deserialized.getAttributes().get("c"), memberInfo.getAttributes().get("c"));
    }
}
