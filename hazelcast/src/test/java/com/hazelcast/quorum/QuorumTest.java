/*
* Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*  http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/

package com.hazelcast.quorum;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.quorum.Quorum;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.QuorumFunction;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QuorumTest extends HazelcastTestSupport {


    @Test(expected = QuorumException.class)
    public void testCustomQuorumFunctionFails() throws Exception {
        Config config = new Config();
        QuorumConfig quorumConfig = new QuorumConfig();
        String quorumName = randomString();
        quorumConfig.setName(quorumName);
        quorumConfig.setEnabled(true);
        quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.addQuorumConfig(quorumConfig);
        String mapName = randomMapName();
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setQuorumName(quorumName);
        config.addMapConfig(mapConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        map.put("1", "1");
    }

    @Test
    public void testCustomQuorumFunctionIsPresent() throws Exception {
        Config config = new Config();
        QuorumConfig quorumConfig = new QuorumConfig();
        String quorumName = randomString();
        quorumConfig.setName(quorumName);
        quorumConfig.setEnabled(true);
        quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.addQuorumConfig(quorumConfig);
        String mapName = randomMapName();
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setQuorumName(quorumName);
        config.addMapConfig(mapConfig);
        HazelcastInstance hazelcastInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        try {
            map.put("1", "1");
            fail();
        } catch (Exception e) {
        }
        Quorum quorum = hazelcastInstance.getQuorumService().getQuorum(quorumName);
        assertFalse(quorum.isPresent());
    }


    @Test(expected = QuorumException.class)
    public void testCustomQuorumFunctionFailsForAllNodes() throws Exception {
        Config config = new Config();
        QuorumConfig quorumConfig = new QuorumConfig();
        String quorumName = randomString();
        quorumConfig.setName(quorumName);
        quorumConfig.setEnabled(true);
        quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return false;
            }
        });
        config.addQuorumConfig(quorumConfig);
        String mapName = randomMapName();
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setQuorumName(quorumName);
        config.addMapConfig(mapConfig);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        IMap<Object, Object> map2 = h2.getMap(mapName);
        map2.put("1", "1");
    }

    @Test
    public void testCustomQuorumFunctionFailsThenSuccess() throws Exception {
        Config config = new Config();
        QuorumConfig quorumConfig = new QuorumConfig();
        String quorumName = randomString();
        quorumConfig.setName(quorumName);
        quorumConfig.setEnabled(true);
        final AtomicInteger count = new AtomicInteger(1);
        quorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                if (count.get() == 1) {
                    count.incrementAndGet();
                    return false;
                } else {
                    return true;
                }
            }
        });
        config.addQuorumConfig(quorumConfig);
        String mapName = randomMapName();
        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setQuorumName(quorumName);
        config.addMapConfig(mapConfig);
        TestHazelcastInstanceFactory f = new TestHazelcastInstanceFactory(2);
        HazelcastInstance hazelcastInstance = f.newHazelcastInstance(config);
        IMap<Object, Object> map = hazelcastInstance.getMap(mapName);
        try {
            map.put("1", "1");
            fail();
        } catch (Exception e) {
            e.printStackTrace();
        }
        HazelcastInstance hazelcastInstance2 = f.newHazelcastInstance(config);
        map.put("1", "1");
        f.shutdownAll();
    }


    @Test
    public void testOneQuorumsFailsOneQuorumSuccessForDifferentMaps() throws Exception {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);
        String fourNodeQuorum = randomString();
        QuorumConfig fourNodeQuorumConfig = new QuorumConfig(fourNodeQuorum, true);
        fourNodeQuorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return members.size() == 4;
            }
        });

        String threeNodeQuorum = randomString();
        QuorumConfig threeNodeQuorumConfig = new QuorumConfig(threeNodeQuorum, true);
        threeNodeQuorumConfig.setQuorumFunctionImplementation(new QuorumFunction() {
            @Override
            public boolean apply(Collection<Member> members) {
                return members.size() == 3;
            }
        });

        MapConfig fourNodeMapConfig = new MapConfig("fourNode");
        fourNodeMapConfig.setQuorumName(fourNodeQuorum);

        MapConfig threeNodeMapConfig = new MapConfig("threeNode");
        threeNodeMapConfig.setQuorumName(threeNodeQuorum);

        Config config = new Config();
        config.addQuorumConfig(threeNodeQuorumConfig);
        config.addQuorumConfig(fourNodeQuorumConfig);
        config.addMapConfig(fourNodeMapConfig);
        config.addMapConfig(threeNodeMapConfig);

        HazelcastInstance h1 = factory.newHazelcastInstance(config);
        HazelcastInstance h2 = factory.newHazelcastInstance(config);
        HazelcastInstance h3 = factory.newHazelcastInstance(config);

        IMap<Object, Object> fourNode = h1.getMap("fourNode");
        IMap<Object, Object> threeNode = h1.getMap("threeNode");
        threeNode.put(generateKeyOwnedBy(h1), "bar");
        try {
            fourNode.put(generateKeyOwnedBy(h1), "bar");
            fail();
        } catch (Exception e) {
        }
    }

}
