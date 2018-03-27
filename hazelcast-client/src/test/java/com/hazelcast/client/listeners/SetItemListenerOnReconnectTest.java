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

package com.hazelcast.client.listeners;

import com.hazelcast.collection.impl.set.SetService;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class SetItemListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private ISet<String> iSet;

    @Override
    String getServiceName() {
        return SetService.SERVICE_NAME;
    }

    @Override
    protected String addListener() {
        iSet = client.getSet(randomString());

        ItemListener<String> listener = new ItemListener<String>() {
            @Override
            public void itemAdded(ItemEvent<String> item) {
                onEvent(item.getItem());
            }

            @Override
            public void itemRemoved(ItemEvent item) {
            }
        };
        return iSet.addItemListener(listener, true);
    }

    @Override
    public void produceEvent(String event) {
        iSet.add(event);
    }

    @Override
    public boolean removeListener(String registrationId) {
        return iSet.removeItemListener(registrationId);
    }
}
