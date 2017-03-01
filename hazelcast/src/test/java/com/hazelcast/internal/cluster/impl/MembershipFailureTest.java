/*
 * Copyright (c) 2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.HEARTBEAT;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MASTER_CONFIRM;
import static com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook.MEMBER_INFO_UPDATE;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.assertMemberViewsAreSame;
import static com.hazelcast.internal.cluster.impl.MembershipUpdateTest.getMemberMap;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsBetween;
import static com.hazelcast.internal.cluster.impl.PacketFiltersUtil.dropOperationsFrom;
import static com.hazelcast.spi.properties.GroupProperty.HEARTBEAT_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_HEARTBEAT_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MembershipFailureTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;

    @Before
    public void init() {
        factory = createHazelcastInstanceFactory();
    }

    // TODO: add membership failure tests
    // ✔ graceful slave shutdown
    // ✔ graceful master shutdown
    // ✔ master removes slave because of heartbeat timeout
    // ✔ slaves remove master because of heartbeat timeout
    // ✔ master removes slave because of master confirm timeout
    // - slave member failure detected by master
    // - slave member suspected by others and its failure eventually detected by master
    // - master member failure detected by others
    // - master and master-candidate fail simultaneously
    // ✔ master fails when master-candidate doesn't have the most recent member list
    // - partial network failure: multiple master claims, eventually split brain and merge
    // - so on...

    @Test
    public void slave_shutdown() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        slave1.shutdown();

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(master), master);
        assertMaster(getAddress(master), slave2);
        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave2));
    }

    @Test
    public void master_shutdown() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        master.shutdown();

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void slave_crash() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        terminateInstance(slave1);

        assertMaster(getAddress(master), master);
        assertMaster(getAddress(master), slave2);
        assertMemberViewsAreSame(getMemberMap(master), getMemberMap(slave2));
    }

    @Test
    public void master_crash() {
        HazelcastInstance master = newHazelcastInstance();
        HazelcastInstance slave1 = newHazelcastInstance();
        HazelcastInstance slave2 = newHazelcastInstance();

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        terminateInstance(master);

        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);

        assertMaster(getAddress(slave1), slave1);
        assertMaster(getAddress(slave1), slave2);
        assertMemberViewsAreSame(getMemberMap(slave1), getMemberMap(slave2));
    }

    @Test
    public void slave_heartbeat_timeout() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                     .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        dropOperationsFrom(slave2, HEARTBEAT);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(1, slave2);
    }

    @Test
    public void master_heartbeat_timeout() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                    .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                                    .setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "3");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        dropOperationsFrom(master, HEARTBEAT);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        assertClusterSizeEventually(1, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(2, slave2);
    }

    @Test
    public void slave_master_confirmation_timeout() {
        Config config = new Config().setProperty(MAX_NO_HEARTBEAT_SECONDS.getName(), "15")
                                    .setProperty(HEARTBEAT_INTERVAL_SECONDS.getName(), "1")
                                    .setProperty(MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), "15")
                                    .setProperty(MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), "1");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);
        HazelcastInstance slave2 = newHazelcastInstance(config);

        dropOperationsFrom(slave2, MASTER_CONFIRM);

        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);
        assertClusterSizeEventually(1, slave2);
    }

    @Test
    public void master_candidate_has_stale_member_list() {
        Config config = new Config().setProperty(MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "5");
        HazelcastInstance master = newHazelcastInstance(config);
        HazelcastInstance slave1 = newHazelcastInstance(config);

        assertClusterSizeEventually(2, master);
        assertClusterSizeEventually(2, slave1);

        HazelcastInstance slave2 = newHazelcastInstance(config);
        assertClusterSizeEventually(3, master);
        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);

        dropOperationsBetween(master, slave1, MEMBER_INFO_UPDATE);

        HazelcastInstance slave3 = newHazelcastInstance(config);

        assertClusterSizeEventually(4, slave3);
        assertClusterSizeEventually(4, slave2);
        assertClusterSize(3, slave1);

        master.getLifecycleService().terminate();

        assertClusterSizeEventually(3, slave1);
        assertClusterSizeEventually(3, slave2);
        assertClusterSizeEventually(3, slave3);

        Address newMasterAddress = getAddress(slave1);
        assertEquals(newMasterAddress, getNode(slave1).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave2).getMasterAddress());
        assertEquals(newMasterAddress, getNode(slave3).getMasterAddress());
    }

    HazelcastInstance newHazelcastInstance() {
        return factory.newHazelcastInstance();
    }

    HazelcastInstance newHazelcastInstance(Config config) {
        return factory.newHazelcastInstance(config);
    }

    private static void assertMaster(Address masterAddress, HazelcastInstance instance) {
        assertEquals(masterAddress, getNode(instance).getMasterAddress());
    }

}
