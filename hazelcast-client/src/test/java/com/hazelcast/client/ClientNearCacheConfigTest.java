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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientNearCacheConfigTest {

    @Test
    public void testSpecificNearCacheConfig_whenAsteriskAtTheEnd(){
        final ClientConfig clientConfig = new ClientConfig();
        final NearCacheConfig genericNearCacheConfig = new NearCacheConfig();
        genericNearCacheConfig.setName("map*");
        clientConfig.addNearCacheConfig(genericNearCacheConfig);

        final NearCacheConfig specificNearCacheConfig = new NearCacheConfig();
        specificNearCacheConfig.setName("mapStudent*");
        clientConfig.addNearCacheConfig(specificNearCacheConfig);

        final NearCacheConfig mapFoo = clientConfig.getNearCacheConfig("mapFoo");
        final NearCacheConfig mapStudentFoo = clientConfig.getNearCacheConfig("mapStudentFoo");

        assertEquals(genericNearCacheConfig, mapFoo);
        assertEquals(specificNearCacheConfig, mapStudentFoo);
    }

    @Test
    public void testSpecificNearCacheConfig_whenAsteriskAtTheBeginning(){
        final ClientConfig clientConfig = new ClientConfig();
        final NearCacheConfig genericNearCacheConfig = new NearCacheConfig();
        genericNearCacheConfig.setName("*Map");
        clientConfig.addNearCacheConfig(genericNearCacheConfig);

        final NearCacheConfig specificNearCacheConfig = new NearCacheConfig();
        specificNearCacheConfig.setName("*MapStudent");
        clientConfig.addNearCacheConfig(specificNearCacheConfig);

        final NearCacheConfig mapFoo = clientConfig.getNearCacheConfig("fooMap");
        final NearCacheConfig mapStudentFoo = clientConfig.getNearCacheConfig("fooMapStudent");

        assertEquals(genericNearCacheConfig, mapFoo);
        assertEquals(specificNearCacheConfig, mapStudentFoo);
    }

    @Test
    public void testSpecificNearCacheConfig_whenAsteriskInTheMiddle(){
        final ClientConfig clientConfig = new ClientConfig();
        final NearCacheConfig genericNearCacheConfig = new NearCacheConfig();
        genericNearCacheConfig.setName("map*Bar");
        clientConfig.addNearCacheConfig(genericNearCacheConfig);

        final NearCacheConfig specificNearCacheConfig = new NearCacheConfig();
        specificNearCacheConfig.setName("mapStudent*Bar");
        clientConfig.addNearCacheConfig(specificNearCacheConfig);

        final NearCacheConfig mapFoo = clientConfig.getNearCacheConfig("mapFooBar");
        final NearCacheConfig mapStudentFoo = clientConfig.getNearCacheConfig("mapStudentFooBar");

        assertEquals(genericNearCacheConfig, mapFoo);
        assertEquals(specificNearCacheConfig, mapStudentFoo);
    }

}
