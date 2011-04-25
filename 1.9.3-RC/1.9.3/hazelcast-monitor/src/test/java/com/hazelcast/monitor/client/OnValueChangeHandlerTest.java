/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.monitor.client;

import com.google.gwt.user.client.rpc.AsyncCallback;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.*;

public class OnValueChangeHandlerTest {
    @Test
    public void testMapPage() throws Exception {
        HazelcastServiceAsync hazelcastService = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(hazelcastService);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        Map<Integer, ClusterWidgets> mapOfCLusterWidgets = new HashMap<Integer, ClusterWidgets>();
        ClusterWidgets clusterWidgets = mock(ClusterWidgets.class);
        mapOfCLusterWidgets.put(0, clusterWidgets);
        when(hazelcastMonitor.getMapClusterWidgets()).thenReturn(mapOfCLusterWidgets);
        OnValueChangeHandler handler = new OnValueChangeHandler(servicesFactory, hazelcastMonitor);
        ConfigLink configLink = new ConfigLink();
        configLink.setClusterId(0);
        configLink.setType("MAP");
        configLink.setName("MyMAP");
        handler.mapPageBuilders.clear();
        PageBuilder pageBuilder = mock(PageBuilder.class);
        handler.mapPageBuilders.put("MAP", pageBuilder);
        handler.handle(configLink);
        verify(pageBuilder, times(1)).buildPage(eq(clusterWidgets), eq("MyMAP"), any(AsyncCallback.class), eq(servicesFactory));
        verify(clusterWidgets, times(1)).deRegisterAll();
    }
}
