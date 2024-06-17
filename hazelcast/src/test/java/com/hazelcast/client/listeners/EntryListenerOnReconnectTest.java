/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;

import static com.hazelcast.client.impl.connection.tcp.RoutingMode.SMART;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.UNISOCKET;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private IMap<String, String> iMap;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(UNISOCKET, SMART);
    }

    @Override
    String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    protected UUID addListener() {
        iMap = client.getMap(randomString());

        final EntryAdapter<String, String> listener = new EntryAdapter<>() {
            public void onEntryEvent(EntryEvent<String, String> event) {
                onEvent(event.getKey());
            }
        };
        return iMap.addEntryListener(listener, true);
    }

    @Override
    public void produceEvent(String event) {
        iMap.put(event, randomString());
    }

    @Override
    public boolean removeListener(UUID registrationId) {
        return iMap.removeEntryListener(registrationId);
    }
}
