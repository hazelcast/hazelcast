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

package com.hazelcast.cache;

import com.hazelcast.config.CacheConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.cache.Cache;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * @author asimarslan
 */
@RunWith(HazelcastSerialClassRunner.class)
//@Category(QuickTest.class)
public class BasicCacheTest extends HazelcastTestSupport {

    HazelcastInstance hz;
    HazelcastInstance hz2;

    @Before
    public void init() {
        //FIXME REMOVE HERE
//        final String logging = "hazelcast.logging.type";
//        if (System.getProperty(logging) == null) {
//            System.setProperty(logging, "log4j");
//        }

        System.setProperty("hazelcast.version.check.enabled", "false");
        System.setProperty("hazelcast.mancenter.enabled", "false");
        System.setProperty("hazelcast.wait.seconds.before.join", "1");
        System.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        System.setProperty("java.net.preferIPv4Stack", "true");
//        System.setProperty("hazelcast.jmx", "true");
        System.setProperty("hazelcast.partition.count", "2");

        // randomize multicast group...
        Random rand = new Random();
        int g1 = rand.nextInt(255);
        int g2 = rand.nextInt(255);
        int g3 = rand.nextInt(255);
        System.setProperty("hazelcast.multicast.group", "224." + g1 + "." + g2 + "." + g3);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        Config config = new Config();
        hz= factory.newHazelcastInstance(config);
//        hz2= factory.newHazelcastInstance(config);
    }

/*    @Test
    public void testJSRExample1(){
        CachingProvider cachingProvider = Caching.getCachingProvider();
        assertNotNull(cachingProvider);

        CacheManager cacheManager = cachingProvider.getCacheManager();
        assertNotNull(cacheManager);

        MutableConfiguration<String, Integer> config =  new MutableConfiguration<String, Integer>();

        cacheManager.createCache("simpleCache", config);
        Cache<String, Integer> cache = cacheManager.getCache("simpleCache");
        String key = "key";
        Integer value1 = 1;
        cache.put("key", value1);


        Integer value2 = cache.get(key);

        assertEquals(value1,value2);
    }*/

//    @Test
    public void testJSRExample1(){

        HazelcastCachingProvider hcp = new HazelcastCachingProvider();

        HazelcastCacheManager cacheManager = new HazelcastCacheManager(hcp,hz,hcp.getDefaultURI(),hcp.getDefaultClassLoader(),null);

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        config.setName("SimpleCache");
        config.setInMemoryFormat(InMemoryFormat.OBJECT);

        Cache<Integer, String> simpleCache = cacheManager.createCache("simpleCache", config);

        Cache<Integer, String> cache = cacheManager.getCache("simpleCache");

        Integer key = 1;
        String value1 = "value";
        cache.put(key, value1);


        String value2 = cache.get(key);

        assertEquals(value1,value2);

        cache.remove(key);

        assertNull(cache.get(key));
    }

    @Test
    public void testIterator(){

        HazelcastCachingProvider hcp = new HazelcastCachingProvider();

        HazelcastCacheManager cacheManager = new HazelcastCacheManager(hcp,hz,hcp.getDefaultURI(),hcp.getDefaultClassLoader(),null);

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        config.setName("SimpleCache");
        config.setInMemoryFormat(InMemoryFormat.OBJECT);

        Cache<Integer, String> simpleCache = cacheManager.createCache("simpleCache", config);

        Cache<Integer, String> cache = cacheManager.getCache("simpleCache");

        int testSize=100;
        for(int i=0;i<testSize;i++){
            Integer key = i;
            String value1 = "value"+i;
            cache.put(key, value1);

        }

        final Iterator<Cache.Entry<Integer, String>> iterator = cache.iterator();
        assertNotNull(iterator);

        HashMap<Integer, String> resultMap= new HashMap<Integer, String>();

        int c=0;
        while (iterator.hasNext()){
            final Cache.Entry<Integer, String> next = iterator.next();
            final Integer key = next.getKey();
            final String value = next.getValue();
            resultMap.put(key, value);
            c++;
            if(c>98){
                System.out.print("ready");
            }
        }
        assertEquals(testSize, resultMap.size());

    }

    @Test
    public void testCacheMigration(){
        HazelcastCachingProvider hcp = new HazelcastCachingProvider();

        HazelcastCacheManager cacheManager = new HazelcastCacheManager(hcp,hz,hcp.getDefaultURI(),hcp.getDefaultClassLoader(),null);

        CacheConfig<Integer,String> config =  new CacheConfig<Integer, String>();
        config.setName("SimpleCache");
        config.setInMemoryFormat(InMemoryFormat.OBJECT);

        Cache<Integer, String> simpleCache = cacheManager.createCache("simpleCache", config);

        Cache<Integer, String> cache = cacheManager.getCache("simpleCache");

        for(int i=0;i<100;i++){
            cache.put(i,"value"+i);
        }

        hz2.shutdown();


        for(int i=0;i<100;i++){
            String val = cache.get(i);
            assertEquals(val,"value" + i);
        }

    }


}
