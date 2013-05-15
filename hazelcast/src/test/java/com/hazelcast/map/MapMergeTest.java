/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.StaticNodeFactory;
import com.hazelcast.map.merge.HigherHitsMapMergePolicy;

import java.util.Map;
import java.util.Set;

public class MapMergeTest {

    public static void main(String[] args) throws InterruptedException {
        StaticNodeFactory factory = new StaticNodeFactory(2);
        Config cfg1 = new Config();
        cfg1.setProperty("hazelcast.merge.first.run.delay.seconds","0");
        cfg1.setProperty("hazelcast.merge.next.run.delay.seconds","1");
        cfg1.getGroupConfig().setName("group1");
        cfg1.getMapConfig("default").setMergePolicy(HigherHitsMapMergePolicy.class.getName());
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg1);

        Config cfg2 = new Config();
        cfg2.setProperty("hazelcast.merge.first.run.delay.seconds","0");
        cfg2.setProperty("hazelcast.merge.next.run.delay.seconds","1");
        cfg2.getGroupConfig().setName("group2");
        cfg2.getMapConfig("default").setMergePolicy(HigherHitsMapMergePolicy.class.getName());
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg2);

        Map map1 = instance1.getMap("map");
        Map map2 = instance2.getMap("map");

        map1.put(1,"one");
        map1.put(3,"three");
        map1.put(5,"five");
        map1.get(1);
        map1.get(1);
        map1.get(3);
        map1.get(3);

        map2.put(1,"ONE");
        map2.put(2,"TWO");
        map2.put(3,"THREE");
        map2.put(4,"FOUR");
        map2.put(5,"FIVE");
        map2.put(6,"SIX");
        for (int i = 0; i < 100; i++) {
            map2.get(5);
            map2.get(5);
            map2.get(5);
            map2.get(5);
        }

        Thread.sleep(1000);

        instance2.getConfig().getGroupConfig().setName("group1");
        System.out.println("ready to merge");

        for (int i = 0; i < 200; i++) {
            Thread.sleep(1000);
            System.out.println("wait:" + i);
            System.out.println("wait:" + i);
            System.out.println("wait:"+i);
            Set keys = map1.keySet();
            for (Object key : keys) {
                System.out.println("key:"+key+ " value:"+map1.get(key));
            }
        }

    }
}
