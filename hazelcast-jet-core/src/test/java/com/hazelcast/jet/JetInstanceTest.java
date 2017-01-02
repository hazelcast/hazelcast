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

package com.hazelcast.jet;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.impl.connector.IMapReader;
import com.hazelcast.jet.impl.connector.IMapWriter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.TestUtil.executeAndPeel;
import static org.junit.Assert.assertEquals;

@Category(QuickTest.class)
@RunWith(HazelcastSerialClassRunner.class)
public class JetInstanceTest extends JetTestSupport {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @After
    public void shutdown() {
        Jet.shutdownAll();
    }

    @Test
    public void when_twoJetInstancesCreated_then_clusterOfTwoShouldBeFormed()
    {
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        assertEquals(2, instance1.getCluster().getMembers().size());
    }

    @Test
    public void when_twoJetAndTwoHzInstancesCreated_then_twoClusterSOfTwoShouldBeFormed()
    {
        JetInstance jetInstance1 = Jet.newJetInstance();
        JetInstance jetInstance2 = Jet.newJetInstance();

        HazelcastInstance hazelcastInstance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hazelcastInstance2 = Hazelcast.newHazelcastInstance();

        assertEquals(2, jetInstance1.getCluster().getMembers().size());
        assertEquals(2, hazelcastInstance1.getCluster().getMembers().size());
    }

    @Test
    public void when_jetClientCreated_then_doesNotConnectToHazelcastCluster()
    {
        Hazelcast.newHazelcastInstance();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Unable to connect");
        Jet.newJetClient();
    }

    @Test
    public void when_hazelcastClientCreated_then_doesNotConnectToJetCluster()
    {
        Jet.newJetInstance();

        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Unable to connect");
        HazelcastClient.newHazelcastClient();
    }
}
