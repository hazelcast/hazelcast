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

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.config.WanTargetClusterConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.merge.PassThroughMergePolicy;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class WanReplicationTest {

    @BeforeClass
    public static void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testWANClustering() throws Exception {
        Config c1 = new Config();
        Config c2 = new Config();
        c1.getGroupConfig().setName("newyork");
        c1.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5702").setGroupName("london")));
        c1.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        c2.getGroupConfig().setName("london");
        c2.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5701").setGroupName("newyork")));
        c2.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h12 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h13 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h22 = Hazelcast.newHazelcastInstance(c2);
        int size = 1000;
        for (int i = 0; i < size; i++) {
            h2.getMap("default").put(i, "value" + i);
            h22.getMap("default").put(size + i, "value" + (size + i));
        }
        Thread.sleep(5000);
        Assert.assertEquals(2 * size, h1.getMap("default").size());
        Assert.assertEquals(2 * size, h2.getMap("default").size());
        Assert.assertEquals(2 * size, h12.getMap("default").size());
        Assert.assertEquals(2 * size, h13.getMap("default").size());
        Assert.assertEquals(2 * size, h22.getMap("default").size());
        for (int i = 0; i < size / 2; i++) {
            h1.getMap("default").remove(i);
            h13.getMap("default").remove(size + i);
        }
        Thread.sleep(5000);
        Assert.assertEquals(size, h1.getMap("default").size());
        Assert.assertEquals(size, h2.getMap("default").size());
        Assert.assertEquals(size, h12.getMap("default").size());
        Assert.assertEquals(size, h13.getMap("default").size());
        Assert.assertEquals(size, h22.getMap("default").size());
    }

    @Test
    public void testWANClustering2() throws Exception {
        Config c1 = new Config();
        Config c2 = new Config();
        c1.getGroupConfig().setName("newyork");
        c1.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5703").setGroupName("london")));
        c1.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        c2.getGroupConfig().setName("london");
        c2.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5701").setGroupName("newyork")));
        c2.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        HazelcastInstance h10 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h11 = Hazelcast.newHazelcastInstance(c1);
        int size = 1000;
        for (int i = 0; i < size; i++) {
            h11.getMap("default").put(i, "value" + i);
        }
        HazelcastInstance h20 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h21 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h12 = Hazelcast.newHazelcastInstance(c1);
        Thread.sleep(5000);
        Assert.assertEquals(size, h10.getMap("default").size());
        Assert.assertEquals(size, h20.getMap("default").size());
        Assert.assertEquals(size, h12.getMap("default").size());
        Assert.assertEquals(size, h11.getMap("default").size());
        Assert.assertEquals(size, h21.getMap("default").size());
        for (int i = 0; i < size; i++) {
            h21.getMap("default").put(size + i, "value" + (size + i));
        }
        Thread.sleep(5000);
        Assert.assertEquals(2 * size, h10.getMap("default").size());
        Assert.assertEquals(2 * size, h20.getMap("default").size());
        Assert.assertEquals(2 * size, h12.getMap("default").size());
        Assert.assertEquals(2 * size, h11.getMap("default").size());
        Assert.assertEquals(2 * size, h21.getMap("default").size());
        for (int i = 0; i < size / 2; i++) {
            h10.getMap("default").remove(i);
            h21.getMap("default").remove(size + i);
        }
        Thread.sleep(5000);
        Assert.assertEquals(size, h10.getMap("default").size());
        Assert.assertEquals(size, h20.getMap("default").size());
        Assert.assertEquals(size, h12.getMap("default").size());
        Assert.assertEquals(size, h11.getMap("default").size());
        Assert.assertEquals(size, h21.getMap("default").size());
    }

    @Test
    public void testWANClusteringActivePassive() throws Exception {
        Config c1 = new Config();
        Config c2 = new Config();
        c1.getGroupConfig().setName("newyork");
        c1.addWanReplicationConfig(new WanReplicationConfig()
                .setName("my-wan")
                .addTargetClusterConfig(new WanTargetClusterConfig()
                        .addEndpoint("127.0.0.1:5702").setGroupName("london")));
        c1.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        c2.getGroupConfig().setName("london");
        c2.getMapConfig("default").setWanReplicationRef(new WanReplicationRef()
                .setName("my-wan")
                .setMergePolicy(PassThroughMergePolicy.NAME));
        HazelcastInstance h10 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h20 = Hazelcast.newHazelcastInstance(c2);
        int size = 1000;
        for (int i = 0; i < size; i++) {
            h10.getMap("default").put(size + i, "value" + (size + i));
        }
        Thread.sleep(5000);
        Assert.assertEquals(size, h10.getMap("default").size());
        Assert.assertEquals(size, h20.getMap("default").size());
    }
}
