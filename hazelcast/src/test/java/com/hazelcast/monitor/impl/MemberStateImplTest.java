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

package com.hazelcast.monitor.impl;

import com.hazelcast.cache.impl.CacheStatisticsImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.management.dto.ClientEndPointDTO;
import com.hazelcast.monitor.HotRestartState;
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberStateImplTest extends HazelcastTestSupport {

    @Test
    public void testDefaultConstructor() {
        MemberStateImpl memberState = new MemberStateImpl();

        assertNotNull(memberState.toString());
    }

    @Test
    public void testSerialization() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance();

        LocalReplicatedMapStatsImpl replicatedMapStats = new LocalReplicatedMapStatsImpl(true);
        replicatedMapStats.incrementPuts(30);
        CacheStatisticsImpl cacheStatistics = new CacheStatisticsImpl(Clock.currentTimeMillis());
        cacheStatistics.increaseCacheHits(5);

        Collection<ClientEndPointDTO> clients = new ArrayList<ClientEndPointDTO>();
        ClientEndPointDTO client = new ClientEndPointDTO();
        client.uuid = "abc123456";
        client.address = "localhost";
        client.clientType = "undefined";
        clients.add(client);

        Map<String, Long> runtimeProps = new HashMap<String, Long>();
        runtimeProps.put("prop1", 598123L);

        final String backupDirectory = "/hot/backup/dir";
        final HotRestartStateImpl hotRestartState = new HotRestartStateImpl(backupDirectory);

        TimedMemberStateFactory factory = new TimedMemberStateFactory(getHazelcastInstanceImpl(hazelcastInstance));
        TimedMemberState timedMemberState = factory.createTimedMemberState();

        MemberStateImpl memberState = timedMemberState.getMemberState();
        memberState.setAddress("memberStateAddress:Port");
        memberState.setOperationStats(new LocalOperationStatsImpl());
        memberState.setHotRestartState(hotRestartState);

        MemberStateImpl deserialized = new MemberStateImpl();
        deserialized.fromJson(memberState.toJson());

        assertEquals("memberStateAddress:Port", deserialized.getAddress());
        assertNotNull(deserialized.getOperationStats());

        final HotRestartState deserializedHotRestartState = deserialized.getHotRestartState();
        assertEquals(backupDirectory, deserializedHotRestartState.getBackupDirectory());
    }
}
