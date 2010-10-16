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

package com.hazelcast.client;

import com.hazelcast.client.ClientProperties.ClientPropertyName;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.TestUtility.newHazelcastClient;
import static org.junit.Assert.*;

public class HazelcastClientClusterTest {

    @After
    @Before
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testMembershipListener() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        HazelcastClient client = newHazelcastClient(h1);
        final CountDownLatch memberAddLatch = new CountDownLatch(1);
        final CountDownLatch memberRemoveLatch = new CountDownLatch(1);
        client.getCluster().addMembershipListener(new MembershipListener() {
            public void memberAdded(MembershipEvent membershipEvent) {
                memberAddLatch.countDown();
            }

            public void memberRemoved(MembershipEvent membershipEvent) {
                memberRemoveLatch.countDown();
            }
        });
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        h2.getLifecycleService().shutdown();
        assertTrue(memberAddLatch.await(10, TimeUnit.SECONDS));
        assertTrue(memberRemoveLatch.await(10, TimeUnit.SECONDS));
        client.shutdown();
    }
    
    @Test(expected=IllegalStateException.class, timeout=5000L)
    public void testNoClusterOnStart() throws Exception {
        final ClientProperties clientProperties = 
            ClientProperties.crateBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient.newHazelcastClient(clientProperties, "localhost:5701");
    }
    
    @Test(expected=NoMemberAvailableException.class, timeout=15000L)
    public void testNoClusterAfterStart() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties = 
            ClientProperties.crateBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        try{
            final IMap<Object, Object> map = client.getMap("default");
            map.put("smth", "nothing");
            h1.getLifecycleService().shutdown();
            map.put("smth", "nothing");
        } finally {
            client.getLifecycleService().shutdown();
        }
    }
    
    @Test(timeout=15000L)
    public void testRestartCluster() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties = 
            ClientProperties.crateBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_ATTEMPTS_LIMIT, "5");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "1000");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        try{
            final IMap<String, String> map = client.getMap("default");
            
            final List<String> values = new ArrayList<String>();
            map.addEntryListener(new EntryAdapter<String, String>(){
                @Override
                public void entryAdded(EntryEvent<String, String> event) {
                    values.add(event.getValue());
                }
                @Override
                public void entryUpdated(EntryEvent<String, String> event) {
                    values.add(event.getValue());
                }
            }, true);
            final BlockingQueue<LifecycleState> states = new LinkedBlockingQueue<LifecycleState>();  
            client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
                
                public void stateChanged(LifecycleEvent event) {
                    states.add(event.getState());
                }
            });
            map.put("smth", "nothing1");
            Thread.sleep(50L);
            assertArrayEquals(values.toString(), new String[]{"nothing1"}, values.toArray(new String[0]));
            h1.getLifecycleService().shutdown();
            assertEquals(LifecycleState.CLIENT_CONNECTION_LOST, states.poll(500L, TimeUnit.MILLISECONDS));
            Thread.sleep(50L);
            try{
                map.put("smth", "nothing2");
                fail();
            } catch(NoMemberAvailableException e){
            }
            try{
                map.put("smth", "nothing3");
                fail();
            } catch(NoMemberAvailableException e){
            }
            h1 = Hazelcast.newHazelcastInstance(new Config());
            
            assertEquals(LifecycleState.CLIENT_CONNECTION_OPENED, states.poll(500L, TimeUnit.MILLISECONDS));
            map.put("smth", "nothing4");
            Thread.sleep(50L);
            assertArrayEquals(values.toString(), new String[]{"nothing1", "nothing4"}, values.toArray(new String[0]));
        } finally {
            client.getLifecycleService().shutdown();
        }
    }
    
    @Test(timeout=20000L)
    public void testRestartClusterTwice() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties = 
            ClientProperties.crateBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_ATTEMPTS_LIMIT, "5");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "1000");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        try{
        final IMap<String, String> map = client.getMap("default");
        final List<String> values = new ArrayList<String>();
        map.addEntryListener(new EntryAdapter<String, String>(){
            @Override
            public void entryAdded(EntryEvent<String, String> event) {
                values.add(event.getValue());
            }
            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                values.add(event.getValue());
            }
        }, true);
        final BlockingQueue<LifecycleState> states = new LinkedBlockingQueue<LifecycleState>();  
        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            
            public void stateChanged(LifecycleEvent event) {
                states.add(event.getState());
            }
        });
        map.put("smth", "nothing");
        for(int i = 0; i < 2; i++){
            h1.getLifecycleService().shutdown();
            assertEquals(LifecycleState.CLIENT_CONNECTION_LOST, states.poll(500L, TimeUnit.MILLISECONDS));
            try{
                map.put("smth", "nothing-" + i);
                fail();
            } catch(NoMemberAvailableException e){
            }
            Thread.sleep(50L);
            try{
                map.put("smth", "nothing_" + i);
                fail();
            } catch(NoMemberAvailableException e){
            }
            Thread.sleep(50L);
            h1 = Hazelcast.newHazelcastInstance(new Config());
            assertEquals(LifecycleState.CLIENT_CONNECTION_OPENED, states.poll(500L, TimeUnit.MILLISECONDS));
            
            map.put("smth", "nothing" + i);
            Thread.sleep(50L);
        }
        assertArrayEquals(values.toString(), new String[]{"nothing", "nothing0", "nothing1"}, values.toArray(new String[0]));
        } finally {
            client.getLifecycleService().shutdown();
        }
    }
    
    @Test(expected=NoMemberAvailableException.class)
    public void testNoClusterAfterStartIssue328() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        final ClientProperties clientProperties = 
            ClientProperties.crateBaseClientProperties(GroupConfig.DEFAULT_GROUP_NAME, GroupConfig.DEFAULT_GROUP_PASSWORD);
        clientProperties.setPropertyValue(ClientPropertyName.INIT_CONNECTION_ATTEMPTS_LIMIT, "2");
        clientProperties.setPropertyValue(ClientPropertyName.RECONNECTION_TIMEOUT, "500");
        HazelcastClient client = newHazelcastClient(clientProperties, h1);
        try{
            final IMap<Object, Object> map = client.getMap("default");
            h1.getLifecycleService().shutdown();
            map.put("smth", "nothing");
        } finally {
            client.getLifecycleService().shutdown();
        }
    }
}
