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

package com.hazelcast.monitor;

import com.google.gwt.user.client.rpc.AsyncCallback;
import com.hazelcast.monitor.client.*;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class MapStatsPanelTest {

    @Test
    public void testRegisterOnePanel() throws Exception {
        AsyncCallback<ChangeEvent> mockCallBack = mock(AsyncCallback.class);
        String name = "myMap";
        HazelcastServiceAsync mockHazelcastServiceAsync = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(mockHazelcastServiceAsync);
        MonitoringPanel monitoringPanel = new MapTimesPanel(name, mockCallBack, servicesFactory);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        ClusterView clusterView = new ClusterView();
        clusterView.setId(1);
        ClusterWidgets clusterWidgets = new ClusterWidgets(hazelcastMonitor, clusterView);
        monitoringPanel.register(clusterWidgets);
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(ChangeEventType.MAP_STATISTICS);
        assertEquals(1, list.size());
        assertTrue(list.contains(monitoringPanel));
        verify(mockHazelcastServiceAsync, times(1)).registerEvent(ChangeEventType.MAP_STATISTICS, 1, name,
                mockCallBack);
    }

    @Test
    public void testRegisterTwoPanels() throws Exception {
        AsyncCallback<ChangeEvent> mockCallBack = mock(AsyncCallback.class);
        String name = "myMap";
        HazelcastServiceAsync mockHazelcastServiceAsync = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(mockHazelcastServiceAsync);
        MonitoringPanel monitoringPanel1 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        ClusterView clusterView = new ClusterView ();
        clusterView.setId(1);
        ClusterWidgets clusterWidgets = new ClusterWidgets(hazelcastMonitor, clusterView);
        monitoringPanel1.register(clusterWidgets);
        List<MonitoringPanel> list = clusterWidgets.getPanels().get(ChangeEventType.MAP_STATISTICS);
        assertTrue(list.size() == 1);
        assertTrue(list.contains(monitoringPanel1));
        verify(mockHazelcastServiceAsync, times(1)).registerEvent(ChangeEventType.MAP_STATISTICS, 1, name,
                mockCallBack);
        MonitoringPanel monitoringPanel2 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        monitoringPanel2.register(clusterWidgets);
        assertTrue(list.size() == 2);
        assertTrue(list.contains(monitoringPanel2));
        verify(mockHazelcastServiceAsync, times(1)).registerEvent(ChangeEventType.MAP_STATISTICS, 1, name,
                mockCallBack);
    }

    @Test
    public void testRegisterOnePanleAndDeregisterIt() throws Exception {
        AsyncCallback<ChangeEvent> mockCallBack = mock(AsyncCallback.class);
        String name = "myMap";
        HazelcastServiceAsync mockHazelcastServiceAsync = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(mockHazelcastServiceAsync);
        MonitoringPanel monitoringPanel = new MapTimesPanel(name, mockCallBack, servicesFactory);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        ClusterView clusterView = new ClusterView();
        clusterView.setId(1);
        ClusterWidgets clusterWidgets = new ClusterWidgets(hazelcastMonitor, clusterView);
        List<MonitoringPanel> list = new ArrayList<MonitoringPanel>();
        list.add(monitoringPanel);
        clusterWidgets.getPanels().put(ChangeEventType.MAP_STATISTICS, list);
        monitoringPanel.deRegister(clusterWidgets);
        assertTrue(list.size() == 0);
        verify(mockHazelcastServiceAsync, times(1)).deRegisterEvent(eq(ChangeEventType.MAP_STATISTICS), eq(1), eq(name),
                Matchers.<AsyncCallback<Void>>any());
    }

    @Test
    public void testRegisterTwoPanelsAndDeRegisterOne() throws Exception {
        AsyncCallback<ChangeEvent> mockCallBack = mock(AsyncCallback.class);
        String name = "myMap";
        HazelcastServiceAsync mockHazelcastServiceAsync = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(mockHazelcastServiceAsync);
        MonitoringPanel monitoringPanel = new MapTimesPanel(name, mockCallBack, servicesFactory);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        ClusterView clusterView = new ClusterView();
        clusterView.setId(1);
        ClusterWidgets clusterWidgets = new ClusterWidgets(hazelcastMonitor, clusterView);
        MonitoringPanel monitoringPanel2 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        monitoringPanel2.register(clusterWidgets);
        List<MonitoringPanel> list = new ArrayList<MonitoringPanel>();
        list.add(monitoringPanel);
        list.add(monitoringPanel2);
        clusterWidgets.getPanels().put(ChangeEventType.MAP_STATISTICS, list);
        monitoringPanel.deRegister(clusterWidgets);
        assertTrue(list.size() == 1);
        assertTrue(list.contains(monitoringPanel2));
        verify(mockHazelcastServiceAsync, times(0)).deRegisterEvent(eq(ChangeEventType.MAP_STATISTICS), eq(1), eq(name),
                Matchers.<AsyncCallback<Void>>any());
    }

    @Test
    public void testRegisterTwoPanelsAndDeRegisterBoth() throws Exception {
        AsyncCallback<ChangeEvent> mockCallBack = mock(AsyncCallback.class);
        String name = "myMap";
        HazelcastServiceAsync mockHazelcastServiceAsync = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(mockHazelcastServiceAsync);
        MonitoringPanel monitoringPanel1 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        ClusterView clusterView = new ClusterView();
        clusterView.setId(1);
        ClusterWidgets clusterWidgets = new ClusterWidgets(hazelcastMonitor, clusterView);
        MonitoringPanel monitoringPanel2 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        monitoringPanel2.register(clusterWidgets);
        List<MonitoringPanel> list = new ArrayList<MonitoringPanel>();
        list.add(monitoringPanel1);
        list.add(monitoringPanel2);
        clusterWidgets.getPanels().put(ChangeEventType.MAP_STATISTICS, list);
        monitoringPanel1.deRegister(clusterWidgets);
        assertTrue(list.size() == 1);
        assertTrue(list.contains(monitoringPanel2));
        verify(mockHazelcastServiceAsync, times(0)).deRegisterEvent(eq(ChangeEventType.MAP_STATISTICS), eq(1), eq(name),
                Matchers.<AsyncCallback<Void>>any());
        monitoringPanel2.deRegister(clusterWidgets);
        assertTrue(list.size() == 0);
        verify(mockHazelcastServiceAsync, times(1)).deRegisterEvent(eq(ChangeEventType.MAP_STATISTICS), eq(1), eq(name),
                Matchers.<AsyncCallback<Void>>any());
    }

    @Test
    public void testRegisterOnePanleAndDeregisterItThenRegisterNewOne() throws Exception {
        AsyncCallback<ChangeEvent> mockCallBack = mock(AsyncCallback.class);
        String name = "myMap";
        HazelcastServiceAsync mockHazelcastServiceAsync = mock(HazelcastServiceAsync.class);
        ServicesFactory servicesFactory = mock(ServicesFactory.class);
        when(servicesFactory.getHazelcastService()).thenReturn(mockHazelcastServiceAsync);
        MonitoringPanel monitoringPanel1 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        HazelcastMonitor hazelcastMonitor = mock(HazelcastMonitor.class);
        ClusterView clusterView = new ClusterView();
        clusterView.setId(1);
        ClusterWidgets clusterWidgets = new ClusterWidgets(hazelcastMonitor, clusterView);
        List<MonitoringPanel> list = new ArrayList<MonitoringPanel>();
        list.add(monitoringPanel1);
        clusterWidgets.getPanels().put(ChangeEventType.MAP_STATISTICS, list);
        monitoringPanel1.deRegister(clusterWidgets);
        assertTrue(list.size() == 0);
        verify(mockHazelcastServiceAsync, times(1)).deRegisterEvent(eq(ChangeEventType.MAP_STATISTICS), eq(1), eq(name),
                Matchers.<AsyncCallback<Void>>any());
        MonitoringPanel monitoringPanel2 = new MapTimesPanel(name, mockCallBack, servicesFactory);
        monitoringPanel2.register(clusterWidgets);
        assertTrue(list.size() == 1);
        assertTrue(list.contains(monitoringPanel2));
        verify(mockHazelcastServiceAsync, times(1)).registerEvent(ChangeEventType.MAP_STATISTICS, 1, name,
                mockCallBack);
    }
}
