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

package com.hazelcast.cache.jsr;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;
import javax.cache.Caching;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheEntryListenerClientServerTest
        extends org.jsr107.tck.event.CacheEntryListenerClientServerTest {

    /*
These system propreties are necessary if you run jsr tests from IDEA
*/
    static {
        System.setProperty("javax.management.builder.initial","com.hazelcast.cache.impl.TCKMBeanServerBuilder");
        System.setProperty("org.jsr107.tck.management.agentId","TCKMbeanServer");
        System.setProperty(Cache.class.getName(),"com.hazelcast.cache.ICache");
        System.setProperty(Cache.Entry.class.getName().replace('$','.'),"com.hazelcast.cache.impl.CacheEntry");
    }

    @AfterClass
    public static void cleanup(){
        JstTestUtil.cleanup();
    }
}
