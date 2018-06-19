/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.query.impl.IndexInfo;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import static java.util.Arrays.asList;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapIndexSynchronizerTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "MapIndexSynchronizerTest";

    private MapServiceContext mapServiceContext;

    private MapIndexSynchronizer synchronizer;

    @Before
    public void before() {
        HazelcastInstance hazelcastInstance = createHazelcastInstance(getConfig());
        NodeEngine nodeEngine = getNodeEngineImpl(hazelcastInstance);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        mapServiceContext = mapService.getMapServiceContext();

        synchronizer = new MapIndexSynchronizer(mapServiceContext, nodeEngine);
    }

    @Test
    public void noIndexes_properTransition_syncNotFired() {
        synchronizer.onClusterVersionChange(Versions.V3_9);
        assertNoIndexesEventually();

        synchronizer.onClusterVersionChange(Versions.V3_10);
        assertNoIndexesEventually();

    }

    @Test
    public void indexes_properTransition_syncFired() {
        synchronizer.onClusterVersionChange(Versions.V3_9);
        assertNoIndexesEventually();

        IndexInfo orderedIndex = index("age", true);
        IndexInfo unorderedIndex = index("height", false);
        add(orderedIndex, unorderedIndex);
        assertNoIndexesEventually();

        synchronizer.onClusterVersionChange(Versions.V3_10);
        assertIndexesEqualEventually(orderedIndex, unorderedIndex);
    }

    @Test
    public void indexes_improperTransition_syncNotFired() {
        synchronizer.onClusterVersionChange(Version.of(3, 8));
        assertNoIndexesEventually();

        IndexInfo orderedIndex = index("age", true);
        IndexInfo unorderedIndex = index("height", false);
        add(orderedIndex, unorderedIndex);
        assertNoIndexesEventually();

        synchronizer.onClusterVersionChange(Versions.V3_10);
        assertNoIndexesEventually();
    }

    private void assertNoIndexesEventually() {
        Callable assertion = new Callable<Integer>() {
            @Override
            public Integer call() {
                return getIndexDefinitions().size();
            }
        };
        assertEqualsEventually(assertion, 0);
        sleepMillis(500);
        assertEqualsEventually(assertion, 0);
    }

    private void assertIndexesEqualEventually(IndexInfo... indexInfos) {
        final Map<String, Boolean> indexes = indexes(indexInfos);
        assertEqualsEventually(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return getIndexDefinitions().equals(indexes);
            }
        }, true);
    }

    private void add(IndexInfo... indexInfo) {
        mapServiceContext.getMapContainer(MAP_NAME).getPartitionIndexesToAdd().addAll(asList(indexInfo));
    }

    private IndexInfo index(String path, boolean ordered) {
        return new IndexInfo(path, ordered);
    }

    private Map<String, Boolean> indexes(IndexInfo... indexInfos) {
        Map<String, Boolean> indexes = new HashMap<String, Boolean>();
        for (IndexInfo indexInfo : indexInfos) {
            indexes.put(indexInfo.getAttributeName(), indexInfo.isOrdered());
        }
        return indexes;
    }

    private Map<String, Boolean> getIndexDefinitions() {
        return mapServiceContext.getMapContainer(MAP_NAME).getIndexDefinitions();
    }
}
