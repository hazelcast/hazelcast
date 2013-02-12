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

package com.hazelcast.map.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.util.Clock;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;


@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class DynamicClusterTest extends BaseTest {


    @Test
    public void testMapSizeWhileRandomDeaths() throws InterruptedException {
        IMap map = getInstance(0).getMap("testMapSizeWhileRandomDeaths");
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        Random rand = new Random(System.currentTimeMillis());

        for (int i = 0; i < 30; i++) {
            map = getInstance(0).getMap("testMapSizeWhileRandomDeaths");
            assertEquals(map.size(), size);
            System.out.println("trial:"+i+" instance count:"+instanceCount);
            if((rand.nextInt(10)%2 == 0 && instanceCount > 2) || instanceCount > 6) {
                System.out.println("remove...");
                System.out.println("remove...");
                System.out.println("remove...");
                removeInstance();
                System.out.println("removed!!!");
                System.out.println("removed!!!");
                System.out.println("removed!!!");
            }
            else {
                System.out.println("new instance...");
                System.out.println("new instance...");
                System.out.println("new instance...");
                newInstance();
                System.out.println("instance is up!!!");
                System.out.println("instance is up!!!");
                System.out.println("instance is up!!!");
            }
            Thread.sleep(10000);
            int realSize = getInstance(0).getCluster().getMembers().size();
            System.out.println("Instance count Real:" + realSize + " Expected:"+ instanceCount );
            System.out.println("Instance count Real:" + realSize + " Expected:"+ instanceCount );
            System.out.println("Instance count Real:" + realSize + " Expected:"+ instanceCount );
            instanceCount = realSize;
        }


    }

}
