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

package com.hazelcast.monitor.server;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.monitor.client.event.ChangeEvent;
import com.hazelcast.monitor.client.event.ChangeEventType;
import com.hazelcast.monitor.client.event.MapStatistics;
import com.hazelcast.monitor.server.event.ChangeEventGenerator;
import com.hazelcast.monitor.server.event.ChangeEventGeneratorFactory;
import com.hazelcast.monitor.server.event.MapStatisticsGenerator;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HazelcastServiceImplTest {
    @Test
    public void testConnectCluster() throws Exception {
    }

    @Test
    public void testLoadActiveClusterViews() throws Exception {
    }

    @Test
    public void testGetChange() throws Exception {
    }

    @Test
    public void testRegisterMapStatisticsEvent() throws Exception {
        final SessionObject sessionObject = mock(SessionObject.class);
        HazelcastServiceImpl hazelcastService = new HazelcastServiceImpl() {
            @Override
            public SessionObject getSessionObject() {
                return sessionObject;
            }
        };
        HazelcastClient hazelcastClient = mock(HazelcastClient.class);
        ChangeEventGeneratorFactory changeEventGeneratorFactory = mock(ChangeEventGeneratorFactory.class);
        ConcurrentHashMap<Integer, HazelcastClient> hazelcastClientMap = new ConcurrentHashMap<Integer, HazelcastClient>();
        hazelcastClientMap.put(0, hazelcastClient);
        List<ChangeEventGenerator> eventGenerators = new CopyOnWriteArrayList<ChangeEventGenerator>();
//        hazelcastService.sessionObject = sessionObject;
        hazelcastService.changeEventGeneratorFactory = changeEventGeneratorFactory;
        when(sessionObject.getHazelcastClientMap()).thenReturn(hazelcastClientMap);
        when(sessionObject.getEventGenerators()).thenReturn(eventGenerators);
        MapStatisticsGenerator mapStatisticsGenerator = mock(MapStatisticsGenerator.class);
        MapStatistics mapStatistics = mock(MapStatistics.class);
        when(mapStatisticsGenerator.generateEvent()).thenReturn(mapStatistics);
        when(changeEventGeneratorFactory.createEventGenerator(ChangeEventType.MAP_STATISTICS, 0, "myMap",
                hazelcastClient)).thenReturn(mapStatisticsGenerator);
        ChangeEvent changeEvent = hazelcastService.registerEvent(ChangeEventType.MAP_STATISTICS, 0, "myMap");
        assertEquals(mapStatistics, changeEvent);
        assertTrue(eventGenerators.contains(mapStatisticsGenerator));
    }

    @Test
    public void testRegisterMapStatisticsEventTwice() throws Exception {
        final SessionObject sessionObject = mock(SessionObject.class);
        HazelcastServiceImpl hazelcastService = new HazelcastServiceImpl() {
            @Override
            public SessionObject getSessionObject() {
                return sessionObject;
            }
        };
        HazelcastClient hazelcastClient = mock(HazelcastClient.class);
        ChangeEventGeneratorFactory changeEventGeneratorFactory = mock(ChangeEventGeneratorFactory.class);
        ConcurrentHashMap<Integer, HazelcastClient> hazelcastClientMap = new ConcurrentHashMap<Integer, HazelcastClient>();
        hazelcastClientMap.put(0, hazelcastClient);
        List<ChangeEventGenerator> eventGenerators = new CopyOnWriteArrayList<ChangeEventGenerator>();
        hazelcastService.changeEventGeneratorFactory = changeEventGeneratorFactory;
        when(sessionObject.getHazelcastClientMap()).thenReturn(hazelcastClientMap);
        when(sessionObject.getEventGenerators()).thenReturn(eventGenerators);
        MapStatisticsGenerator mapStatisticsGenerator = mock(MapStatisticsGenerator.class);
        MapStatistics mapStatistics = mock(MapStatistics.class);
        when(mapStatisticsGenerator.generateEvent()).thenReturn(mapStatistics);
        when(changeEventGeneratorFactory.createEventGenerator(ChangeEventType.MAP_STATISTICS, 0, "myMap",
                hazelcastClient)).thenReturn(mapStatisticsGenerator);
        ChangeEvent changeEvent = hazelcastService.registerEvent(ChangeEventType.MAP_STATISTICS, 0, "myMap");
        assertEquals(mapStatistics, changeEvent);
        assertTrue(eventGenerators.contains(mapStatisticsGenerator));
        hazelcastService.registerEvent(ChangeEventType.MAP_STATISTICS, 0, "myMap");
        assertTrue(eventGenerators.size() == 1);
    }

    @Test
    public void testDeRegisterMapStatisticsEvent() throws Exception {
        final SessionObject sessionObject = mock(SessionObject.class);
        HazelcastServiceImpl hazelcastService = new HazelcastServiceImpl() {
            @Override
            public SessionObject getSessionObject() {
                return sessionObject;
            }
        };
        ChangeEventGeneratorFactory changeEventGeneratorFactory = mock(ChangeEventGeneratorFactory.class);
        List<ChangeEventGenerator> eventGenerators = new CopyOnWriteArrayList<ChangeEventGenerator>();
        hazelcastService.changeEventGeneratorFactory = changeEventGeneratorFactory;
        when(sessionObject.getEventGenerators()).thenReturn(eventGenerators);
        hazelcastService.deRegisterEvent(ChangeEventType.MAP_STATISTICS, 0, "myMap");
        assertTrue(eventGenerators.isEmpty());
    }
}
