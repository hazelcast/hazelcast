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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class SizeEstimatorTest extends HazelcastTestSupport {

    @Test
    public void testIdleState() throws InterruptedException {
        final String MAP_NAME =  "default";

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance h = factory.newHazelcastInstance(null);

        final IMap<String, String> map = h.getMap(MAP_NAME);

        Assert.assertTrue( map.getLocalMapStats().getHeapCost() == 0);
        Assert.assertTrue( map.getLocalMapStats().getBackupHeapCost() == 0);

        h.getLifecycleService().shutdown();

    }

    @Test
    public void testPutRemove() throws InterruptedException {
        final String MAP_NAME =  "default";

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance h = factory.newHazelcastInstance(null);

        final IMap<String, String> map = h.getMap(MAP_NAME);

        map.put("key","value");

        Assert.assertTrue( map.getLocalMapStats().getHeapCost() > 0);
        Assert.assertTrue( map.getLocalMapStats().getBackupHeapCost() > 0);

        Thread.sleep(1000);

        map.remove("key");

        Thread.sleep(1000);

        Assert.assertTrue( map.getLocalMapStats().getHeapCost() == 0);
        Assert.assertTrue( map.getLocalMapStats().getBackupHeapCost() == 0);

        h.getLifecycleService().shutdown();

    }

    @Test
    public void testBackups() throws InterruptedException {

    }

    @Test
    public void testEvictionPolicy() throws InterruptedException {

    }

    @Test
    public void testNearCache() throws InterruptedException {
        final String NO_NEAR_CAHED_MAP =  "testIssue833";
        final String NEAR_CACHED_MAP =  "testNearCache";

        final Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(MapConfig.InMemoryFormat.BINARY);
        config.getMapConfig( NEAR_CACHED_MAP ).setNearCacheConfig(nearCacheConfig).setBackupCount(0);
        config.getMapConfig( NO_NEAR_CAHED_MAP ).setBackupCount(0);

        final int n = 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory( n );
        final HazelcastInstance h[] = factory.newInstances(config);


        final IMap<String, String> noNearCached = h[0].getMap( NO_NEAR_CAHED_MAP );
        noNearCached.put("key", "value");
        noNearCached.put("key1", "value1");
        noNearCached.put("key2", "value2");
        noNearCached.put("key3", "value3");

        final IMap<String, String> nearCachedMap = h[0].getMap( NEAR_CACHED_MAP );
        nearCachedMap.put("key", "value");
        nearCachedMap.put("key1", "value1");
        nearCachedMap.put("key2", "value2");
        nearCachedMap.put("key3", "value3");

        for (int i= 0; i<100;i++){
            nearCachedMap.get("key");
            nearCachedMap.get("key1");
            nearCachedMap.get("key2");
            nearCachedMap.get("key3");
        }

        Assert.assertTrue(  nearCachedMap.getLocalMapStats().getHeapCost() > noNearCached.getLocalMapStats().getHeapCost() );
        Assert.assertTrue( noNearCached.getLocalMapStats().getBackupHeapCost() == 0);
        Assert.assertTrue( nearCachedMap.getLocalMapStats().getBackupHeapCost() == 0);

        for (int i= 0; i<n;i++){
            h[i].getLifecycleService().shutdown();
        }

    }

    @Test
    public void testInMemoryFormats() throws InterruptedException {
        final String BINARY_MAP =  "testBinaryFormat";
        final String OBJECT_MAP =  "testObjectFormat";
        final String CACHED_MAP =  "testCachedFormat";
        final Config config = new Config();
        config.getMapConfig( BINARY_MAP ).
                setInMemoryFormat(MapConfig.InMemoryFormat.BINARY).setBackupCount(0);
        config.getMapConfig( OBJECT_MAP ).
                setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT).setBackupCount(0);
        config.getMapConfig( CACHED_MAP ).
                setInMemoryFormat(MapConfig.InMemoryFormat.CACHED).setBackupCount(0);

        final int n = 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance[] h = factory.newInstances(config);
        // populate map.
        final IMap<String, String> binaryMap = h[0].getMap(BINARY_MAP);
        binaryMap.put("key", "value");
        binaryMap.put("key1", "value1");
        binaryMap.put("key2", "value2");
        binaryMap.put("key3", "value3");

        final IMap<String, String> objectMap = h[0].getMap(OBJECT_MAP);
        objectMap.put("key", "value");
        objectMap.put("key1", "value1");
        objectMap.put("key2", "value2");
        objectMap.put("key3", "value3");

        final IMap<String, String> cachedMap = h[0].getMap(CACHED_MAP);
        cachedMap.put("key", "value");
        cachedMap.put("key1", "value1");
        cachedMap.put("key2", "value2");
        cachedMap.put("key3", "value3");

        Thread.sleep(2000);
        for(int i = 0; i< n; i++){

            Assert.assertTrue( h[i].getMap(BINARY_MAP).getLocalMapStats().getHeapCost() > 0 );
            Assert.assertTrue( h[i].getMap(BINARY_MAP).getLocalMapStats().getBackupHeapCost() == 0);

            Assert.assertTrue( h[i].getMap(OBJECT_MAP).getLocalMapStats().getHeapCost() == 0 );
            Assert.assertTrue( h[i].getMap(OBJECT_MAP).getLocalMapStats().getBackupHeapCost() == 0);

            Assert.assertTrue(h[i].getMap(CACHED_MAP).getLocalMapStats().getHeapCost() > 0);
            Assert.assertTrue( h[i].getMap(CACHED_MAP).getLocalMapStats().getBackupHeapCost() == 0);
        }

        // clear map
        binaryMap.clear();
        objectMap.clear();
        cachedMap.clear();

        Thread.sleep(2000);

        for(int i = 0; i< n; i++){

            Assert.assertTrue( h[i].getMap(BINARY_MAP).getLocalMapStats().getHeapCost() == 0 );
            Assert.assertTrue( h[i].getMap(BINARY_MAP).getLocalMapStats().getBackupHeapCost() == 0);

            Assert.assertTrue( h[i].getMap(OBJECT_MAP).getLocalMapStats().getHeapCost() == 0 );
            Assert.assertTrue( h[i].getMap(OBJECT_MAP).getLocalMapStats().getBackupHeapCost() == 0);

            Assert.assertTrue( h[i].getMap(CACHED_MAP).getLocalMapStats().getHeapCost() == 0 );
            Assert.assertTrue( h[i].getMap(CACHED_MAP).getLocalMapStats().getBackupHeapCost() == 0);
        }

        for(int i = 0; i< n; i++){

            h[i].getLifecycleService().shutdown();
        }
    }

    public void testIssue833() throws InterruptedException {
        final String MAP_NAME =  "testIssue833";
        Config config = new Config();

        MaxSizeConfig msc = new MaxSizeConfig();
        msc.setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
        msc.setSize( 500 );

        config.getMapConfig( MAP_NAME ).setStatisticsEnabled( true ).setBackupCount( 0 ).setMaxSizeConfig( msc )
                .setInMemoryFormat(MapConfig.InMemoryFormat.BINARY).setEvictionPolicy(MapConfig.EvictionPolicy.LFU);

        /*NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);*/
        int n = 3;
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        HazelcastInstance[] h = factory.newInstances(config);

        IMap<String, String> map = h[0].getMap(MAP_NAME);
        map.put("key", "value");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "asdavalue2");

        Thread.sleep(10000);

        Thread.sleep(3000);
        System.err.println("##########################");
        for(int i = 0; i< n; i++){

            final long heapCost = h[i].getMap(MAP_NAME).getLocalMapStats().getHeapCost();
            System.err.println("heapCost "+i +" : " + heapCost);
            final long backupHeapCost = h[i].getMap(MAP_NAME).getLocalMapStats().getBackupHeapCost();
            System.err.println("backupHeapCost "+i +" : " + +backupHeapCost);
        }

        map.put("key245", "value2");
        Thread.sleep(5000);
        System.err.println("##########################");
        for(int i = 0; i< n; i++){
            final long heapCost = h[i].getMap( MAP_NAME ).getLocalMapStats().getHeapCost();
            System.err.println("heapCost "+i +" : " + heapCost);
            final long backupHeapCost = h[i].getMap( MAP_NAME ).getLocalMapStats().getBackupHeapCost();
            System.err.println("backupHeapCost "+i +" : " + +backupHeapCost);
        }
        map.clear();
        Thread.sleep(3000);
        System.err.println("##########################");
        for(int i = 0; i< n; i++){
            final long heapCost = h[i].getMap( MAP_NAME ).getLocalMapStats().getHeapCost();
            System.err.println("heapCost "+i +" : " + heapCost);
            final long backupHeapCost = h[i].getMap( MAP_NAME ).getLocalMapStats().getBackupHeapCost();
            System.err.println("backupHeapCost "+i +" : " + +backupHeapCost);
        }
//        h[0].getLifecycleService().shutdown();
    }

}
