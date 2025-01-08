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

import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.collection.impl.list.ListService;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.UUID;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListItemListenerOnReconnectTest extends AbstractListenersOnReconnectTest {

    private IList<String> iList;

    @Parameterized.Parameters(name = "{index}: routingMode={0}")
    public static Iterable<?> parameters() {
        return Arrays.asList(RoutingMode.SINGLE_MEMBER, RoutingMode.ALL_MEMBERS);
    }


    @Override
    String getServiceName() {
        return ListService.SERVICE_NAME;
    }

    @Override
    protected UUID addListener() {
        iList = client.getList(randomString());

        ItemListener<String> listener = new ItemListener<>() {
            @Override
            public void itemAdded(ItemEvent<String> item) {
                onEvent(item.getItem());
            }

            @Override
            public void itemRemoved(ItemEvent item) {

            }
        };
        return iList.addItemListener(listener, true);
    }

    @Override
    public void produceEvent(String event) {
        iList.add(event);
    }

    @Override
    public boolean removeListener(UUID registrationId) {
        return iList.removeItemListener(registrationId);
    }
}
