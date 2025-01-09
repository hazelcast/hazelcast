/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.listeners;

import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapEntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ReplicatedMap<String, String> replicatedMap;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }

    @Override
    String getServiceName() {
        return ReplicatedMapService.SERVICE_NAME;
    }

    @Override
    protected UUID addListener() {
        replicatedMap = client.getReplicatedMap(randomString());
        final EntryAdapter<String, String> listener = new EntryAdapter<>() {
            @Override
            public void onEntryEvent(EntryEvent<String, String> event) {
                onEvent(event.getKey());
            }
        };
        return replicatedMap.addEntryListener(listener);
    }

    @Override
    public void produceEvent(String event) {
        replicatedMap.put(event, randomString());
    }

    @Override
    public boolean removeListener(UUID registrationId) {
        return replicatedMap.removeEntryListener(registrationId);
    }

    @Override
    protected void validateRegistrationsOnMembers(final TestHazelcastFactory factory, int expected) {
        assertTrueEventually(() -> {
            boolean found = false;
            for (HazelcastInstance instance : factory.getAllHazelcastInstances()) {
                NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(instance);
                EventServiceImpl eventService = (EventServiceImpl) nodeEngineImpl.getEventService();
                EventServiceSegment serviceSegment = eventService.getSegment(getServiceName(), false);
                if (serviceSegment != null && serviceSegment.getRegistrationIdMap().size() == expected) {
                    found = true;
                }
            }
            assertTrue(found);
        });
    }
}
