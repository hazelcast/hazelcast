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
import com.hazelcast.core.*;
import com.hazelcast.monitor.client.ClusterView;
import com.hazelcast.monitor.client.ConnectionExceptoin;
import com.hazelcast.monitor.client.HazelcastService;
import com.hazelcast.monitor.client.event.ChangeEvent;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HazelcastServerImplTest {

    @Test
    public void connectToClusterGetMap() throws ConnectionExceptoin, InterruptedException {
        HazelcastService service = new HazelcastServiceImpl();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
//        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(null);
        String hostName = h1.getCluster().getLocalMember().getInetSocketAddress().getHostName();
        int port = h1.getCluster().getLocalMember().getInetSocketAddress().getPort();

        ClusterView cv = service.connectCluster("fuad-dev", "dev-pass", hostName + ":" + port);
        Map map1 = h1.getMap("map1");
        Thread.sleep(100);
        List<ChangeEvent> list = service.getChange();
        System.out.println(list.size());
        for (Iterator<ChangeEvent> iterator = list.iterator(); iterator.hasNext();) {
            ChangeEvent changeEvent = iterator.next();
            System.out.println("ChangeEvent: " + changeEvent);
        }
    }

    @Test
    public void connectToClusterDestroyMap() throws ConnectionExceptoin, InterruptedException {
        HazelcastService service = new HazelcastServiceImpl();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        String hostName = h1.getCluster().getLocalMember().getInetSocketAddress().getHostName();
        int port = h1.getCluster().getLocalMember().getInetSocketAddress().getPort();
        service.connectCluster("fuad-dev", "dev-pass", hostName + ":" + port);
        IMap map1 = h1.getMap("map1");
        List<ChangeEvent> list = service.getChange();
        Thread.sleep(100);

        map1.destroy();
        Thread.sleep(100);
        list = service.getChange();
        System.out.println(list.size());
        for (Iterator<ChangeEvent> iterator = list.iterator(); iterator.hasNext();) {
            ChangeEvent changeEvent = iterator.next();
            System.out.println("ChangeEvent: " + changeEvent);
        }
    }

    @Test
    public void listenfromHazelcast() throws ConnectionExceptoin, InterruptedException {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(null);
        String hostName = h1.getCluster().getLocalMember().getInetSocketAddress().getHostName();
        int port = h1.getCluster().getLocalMember().getInetSocketAddress().getPort();

        HazelcastClient client = HazelcastClient.newHazelcastClient("fuad-dev", "dev-pass", hostName + ":" + port);
        client.addInstanceListener(new InstanceListener() {

            public void instanceCreated(InstanceEvent event) {
                System.out.println("Created:" + event);
            }

            public void instanceDestroyed(InstanceEvent event) {
                System.out.println("Destroyed:" + event);
            }

        });
        IMap map1 = h1.getMap("map1");
        map1.destroy();
        Thread.sleep(10000);
    }


}
