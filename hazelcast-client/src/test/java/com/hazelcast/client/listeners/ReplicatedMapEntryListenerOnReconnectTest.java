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

package com.hazelcast.client.listeners;

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ReplicatedMapEntryListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ReplicatedMap<String, String> replicatedMap;

    @Override
    protected String addListener() {
        replicatedMap = client.getReplicatedMap(randomString());
        final EntryAdapter<String, String> listener = new EntryAdapter<String, String>() {
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
    public boolean removeListener(String registrationId) {
        return replicatedMap.removeEntryListener(registrationId);
    }
}
