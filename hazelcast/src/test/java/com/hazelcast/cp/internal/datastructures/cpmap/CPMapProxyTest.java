/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.CPMap;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.exception.CPGroupDestroyedException;
import com.hazelcast.cp.internal.CPSubsystemImpl;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.datastructures.cpmap.proxy.CPMapProxy;
import com.hazelcast.cp.internal.raftop.metadata.GetRaftGroupOp;
import com.hazelcast.cp.internal.raftop.metadata.TriggerDestroyRaftGroupOp;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.spi.exception.DistributedObjectDestroyedException;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class CPMapProxyTest extends HazelcastRaftTestSupport {

    @ClassRule
    public static final ChangeLoggingRule CHANGE_LOGGING_RULE
            = new ChangeLoggingRule("log4j2-cp-purge-debug.xml");

    private static final String MAP_NAME = "mymap";
    private static final String KEY = "k";
    private static final String VALUE = "v";

    HazelcastInstance[] instances;
    CPMap<String, String> map;
    List<MetricsRegistry> metricsRegistries = new ArrayList<>();

    @Before
    public void before() {
        instances = newInstances(3);
        map = getMap();
    }

    private HazelcastInstance getMember() {
        return instances[0];
    }

    private CPSubsystem getCPSubsystem() {
        return getMember().getCPSubsystem();
    }

    private CPMap<String, String> getMap() {
        return getCPSubsystem().getMap(MAP_NAME);
    }

    @Test
    public void testGetNullNameMap() {
        String expectedMessage = "Retrieving a map instance with a null name is not allowed!";
        Throwable actual = assertThrows(NullPointerException.class, () -> getCPSubsystem().getMap(null));
        assertEquals(expectedMessage, actual.getMessage());
    }

    @Test
    public void testGetMap() {
        CPSubsystem cpSubsystem = getCPSubsystem();
        assertNotNull(cpSubsystem);
        assertTrue(cpSubsystem instanceof CPSubsystemImpl);

        assertNotNull(map);
        assertTrue(map instanceof CPMapProxy);

        CPMapProxy<String, String> proxy = (CPMapProxy<String, String>) map;
        assertEquals(CPMapService.SERVICE_NAME, proxy.getServiceName());
        assertEquals(MAP_NAME, proxy.getName());
        assertThrows(UnsupportedOperationException.class, proxy::getPartitionKey);
    }

    @Test
    public void testPutAndGet() {
        assertNull(map.put(KEY, VALUE));
        assertEquals(VALUE, map.get(KEY));
    }

    @Test
    public void testSetAndGet() {
        map.set(KEY, VALUE);
        assertEquals(VALUE, map.get(KEY));
    }

    @Test
    public void testRemove() {
        assertNull(map.put(KEY, VALUE));
        assertEquals(VALUE, map.remove(KEY));
        assertNull(map.remove(KEY));
        assertNull(map.get(KEY));
    }

    @Test
    public void testDelete() {
        assertNull(map.put(KEY, VALUE));
        assertEquals(VALUE, map.get(KEY));
        map.delete(KEY);
        assertNull(map.get(KEY));
    }

    @Test
    public void testPutIfAbsent() {
        assertNull(map.putIfAbsent(KEY, VALUE));
        String v2 = "v2";
        assertEquals(VALUE, map.putIfAbsent(KEY, v2));
        assertEquals(VALUE, map.get(KEY));

        String k2 = "k2";
        map.set(k2, VALUE);
        assertEquals(VALUE, map.putIfAbsent(k2, v2));
        assertEquals(VALUE, map.get(k2));

        String k3 = "k3";
        assertNull(map.putIfAbsent(k3, VALUE));
        assertEquals(VALUE, map.putIfAbsent(k3, v2));
        assertEquals(VALUE, map.remove(k3));
        assertNull(map.putIfAbsent(k3, VALUE));
        assertEquals(VALUE, map.putIfAbsent(k3, v2));
        assertEquals(VALUE, map.get(k3));
    }

    @Test
    public void testNullKeyThrows() {
        String expectedMessage = "argument 'key' cannot be null";

        // put
        Throwable t = Assert.assertThrows(NullPointerException.class, () -> map.put(null, ""));
        assertEquals(expectedMessage, t.getMessage());

        // putIfAbsent
        t = Assert.assertThrows(NullPointerException.class, () -> map.putIfAbsent(null, ""));
        assertEquals(expectedMessage, t.getMessage());

        // set
        t = Assert.assertThrows(NullPointerException.class, () -> map.set(null, ""));
        assertEquals(expectedMessage, t.getMessage());

        // get
        t = Assert.assertThrows(NullPointerException.class, () -> map.get(null));
        assertEquals(expectedMessage, t.getMessage());

        // remove
        t = Assert.assertThrows(NullPointerException.class, () -> map.remove(null));
        assertEquals(expectedMessage, t.getMessage());

        // delete
        t = Assert.assertThrows(NullPointerException.class, () -> map.delete(null));
        assertEquals(expectedMessage, t.getMessage());

        // cas
        t = Assert.assertThrows(NullPointerException.class,
                () -> map.compareAndSet(null, "", ""));
        assertEquals(expectedMessage, t.getMessage());
    }

    @Test
    public void testNullValueThrows() {
        String expectedMessage = "argument 'value' cannot be null";

        // put
        Throwable t = Assert.assertThrows(NullPointerException.class, () -> map.put(KEY, null));
        assertEquals(expectedMessage, t.getMessage());

        // putIfAbsent
        t = Assert.assertThrows(NullPointerException.class, () -> map.putIfAbsent(KEY, null));
        assertEquals(expectedMessage, t.getMessage());

        // set
        t = Assert.assertThrows(NullPointerException.class, () -> map.set(KEY, null));
        assertEquals(expectedMessage, t.getMessage());

        // cas
        expectedMessage = "argument 'expectedValue' cannot be null";
        t = Assert.assertThrows(NullPointerException.class,
                () -> map.compareAndSet(KEY, null, ""));
        assertEquals(expectedMessage, t.getMessage());

        expectedMessage = "argument 'newValue' cannot be null";
        t = Assert.assertThrows(NullPointerException.class,
                () -> map.compareAndSet(KEY, VALUE, null));
        assertEquals(expectedMessage, t.getMessage());
    }

    @Test
    public void testCas() {
        String valueOfFirstCasUpdate = "cas1";
        String valueOfSecondCasUpdate = "cas2";

        assertNull(map.put(KEY, VALUE));
        assertTrue(map.compareAndSet(KEY, VALUE, valueOfFirstCasUpdate));
        assertEquals(valueOfFirstCasUpdate, map.get(KEY));
        assertFalse(map.compareAndSet(KEY, VALUE, valueOfSecondCasUpdate));
        assertEquals(valueOfFirstCasUpdate, map.get(KEY));
        assertTrue(map.compareAndSet(KEY, valueOfFirstCasUpdate, valueOfSecondCasUpdate));
        assertEquals(valueOfSecondCasUpdate, map.get(KEY));
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testUse_afterDestroy() {
        map.destroy();
        map.set(KEY, VALUE);
    }

    @Test(expected = DistributedObjectDestroyedException.class)
    public void testCreate_afterDestroy() {
        map.destroy();

        map = getMap();
        map.set(KEY, VALUE);
    }

    @Test
    public void testMultipleDestroy() {
        map.destroy();
        map.destroy();
    }

    @Test
    public void testRecreate_afterCPGroupDestroy() throws Exception {
        CPGroupId groupId = getGroupId(map);
        map.destroy();

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(() -> {
            CPGroup group = invocationManager.<CPGroup>invoke(getMetadataGroupId(instances[0]), new GetRaftGroupOp(groupId)).join();
            assertEquals(CPGroup.CPGroupStatus.DESTROYED, group.status());
        });

        assertThrows(CPGroupDestroyedException.class, () -> map.get(KEY));

        map = getMap();
        assertNotEquals(groupId, getGroupId(map));

        map.set(KEY, VALUE);
    }

    @Test
    public void testNoResourcesLeaks_afterCPGroupDestroy() throws Exception {
        map.set(KEY, VALUE);
        CPGroupId groupId = getGroupId(map);
        Node node = ((HazelcastInstanceProxy) getLeaderInstance(instances, groupId)).getOriginal().node;

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[0]);
        invocationManager.invoke(getRaftService(instances[0]).getMetadataGroupId(), new TriggerDestroyRaftGroupOp(groupId)).get();

        assertTrueEventually(() -> {
            CPGroup group = invocationManager.<CPGroup>invoke(getMetadataGroupId(instances[0]), new GetRaftGroupOp(groupId)).join();
            assertEquals(CPGroup.CPGroupStatus.DESTROYED, group.status());
        });

        // Should verify that after a CP group is destroyed, all resources that belonged to it are also destroyed
        CPMapService cpMapService = node.nodeEngine.getService(CPMapService.SERVICE_NAME);
        assertFalse("There should be no resources after CP Group Destroy", cpMapService.destroyRaftObject(groupId, MAP_NAME));
    }

    @Test
    public void testNoResourcesLeaks_afterCPReset() {
        map.set(KEY, VALUE);
        CPGroupId groupId = getGroupId(map);
        Node node = ((HazelcastInstanceProxy) getLeaderInstance(instances, groupId)).getOriginal().node;

        getCPSubsystem().getCPSubsystemManagementService().reset().toCompletableFuture().join();

        // Should verify that after a CP reset, all resources are also destroyed
        CPMapService cpMapService = node.nodeEngine.getService(CPMapService.SERVICE_NAME);
        assertFalse("There should be no resources after CP reset", cpMapService.destroyRaftObject(groupId, MAP_NAME));
    }

    private CPGroupId getGroupId(CPMap map) {
        return ((CPMapProxy) map).getGroupId();
    }
}
