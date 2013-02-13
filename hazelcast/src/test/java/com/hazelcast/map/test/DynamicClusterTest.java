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

import com.hazelcast.core.IMap;
import org.junit.Test;
import org.junit.runner.RunWith;
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
                removeInstance();
            }
            else {
                newInstance();
            }
            Thread.sleep(10000);
            int realSize = getInstance(0).getCluster().getMembers().size();
            instanceCount = realSize;
        }


    }

}
