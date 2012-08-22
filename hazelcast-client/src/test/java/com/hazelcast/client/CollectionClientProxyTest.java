/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.impl.ItemListenerManager;
import com.hazelcast.client.impl.ListenerManager;
import com.hazelcast.core.ItemEvent;
import com.hazelcast.core.ItemListener;
import com.hazelcast.impl.ClusterOperation;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.*;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class CollectionClientProxyTest {
    @Test
    public void testIterator() throws Exception {
    }

    @Ignore
    @Test
    public void testAddItemListenerIncludeValue() throws Exception {
        Boolean includeValue = true;
        HazelcastClient client = mock(HazelcastClient.class);
        ListenerManager listenerManager = mock(ListenerManager.class);
        ItemListenerManager itemListenerManager = mock(ItemListenerManager.class);
        when(listenerManager.getItemListenerManager()).thenReturn(itemListenerManager);
        when(client.getListenerManager()).thenReturn(listenerManager);
        when(client.getOutRunnable()).thenReturn(new OutRunnable(client, new HashMap(), new ProtocolWriter()));
        String name = "def";
        PacketProxyHelper proxyHelper = mock(PacketProxyHelper.class);
        when(proxyHelper.getHazelcastClient()).thenReturn(client);
        Packet request = new Packet();
        request.setName(name);
        request.setOperation(ClusterOperation.ADD_LISTENER);
        request.setCallId(1L);
        when(proxyHelper.createCall(request)).thenReturn(new Call(1L, request));
        when(proxyHelper.createRequestPacket(ClusterOperation.ADD_LISTENER, null, null)).thenReturn(request);
        CollectionClientProxy proxy = new SetClientProxy(proxyHelper, name);
        ItemListener listener = new ItemListener() {

            public void itemAdded(ItemEvent itemEvent) {
            }

            public void itemRemoved(ItemEvent itemEvent) {
            }
        };
        proxy.addItemListener(listener, includeValue);
        //verify(listenerManager).addListenerCall(argThat(new CallMatcher()));
        verify(itemListenerManager).registerListener(name, listener, includeValue);
        verify(proxyHelper).doCall(argThat(new CallMatcher()));
    }

    class CallMatcher extends BaseMatcher<Call> {
        public void describeTo(Description description) {
        }

        public boolean matches(Object o) {
            return (o instanceof Call);
        }
    }

    ;

    @Test
    public void testRemoveItemListener() throws Exception {
    }

    @Test
    public void testSize() throws Exception {
    }
}
