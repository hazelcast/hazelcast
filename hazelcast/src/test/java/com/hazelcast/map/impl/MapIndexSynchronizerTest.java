package com.hazelcast.map.impl;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
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

    private static String MAP_NAME = "MapIndexSynchronizerTest";

    private HazelcastInstanceImpl hazelcastInstance;
    private MapServiceContext mapServiceContext;
    private NodeEngine nodeEngine;

    private MapIndexSynchronizer synchronizer;

    @Before
    public void before() {
        hazelcastInstance = ((HazelcastInstanceProxy) createHazelcastInstance(getConfig())).getOriginal();
        nodeEngine = hazelcastInstance.node.getNodeEngine();
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        mapServiceContext = mapService.getMapServiceContext();

        synchronizer = new MapIndexSynchronizer(mapServiceContext, nodeEngine);
    }

    @Test
    public void noIndexes_properTransition_syncNotFired() throws Exception {
        synchronizer.onClusterVersionChange(Versions.V3_8);
        assertNoIndexesEventually();

        synchronizer.onClusterVersionChange(Versions.V3_9);
        assertNoIndexesEventually();

    }

    @Test
    public void indexes_properTransition_syncFired() throws Exception {
        synchronizer.onClusterVersionChange(Versions.V3_8);
        assertNoIndexesEventually();

        IndexInfo orderedIndex = index("age", true);
        IndexInfo unorderedIndex = index("height", false);
        add(orderedIndex, unorderedIndex);
        assertNoIndexesEventually();

        synchronizer.onClusterVersionChange(Versions.V3_9);
        assertIndexesEqualEventually(orderedIndex, unorderedIndex);
    }

    @Test
    public void indexes_improperTransition_syncNotFired() throws Exception {
        synchronizer.onClusterVersionChange(Version.of(3, 7));
        assertNoIndexesEventually();

        IndexInfo orderedIndex = index("age", true);
        IndexInfo unorderedIndex = index("height", false);
        add(orderedIndex, unorderedIndex);
        assertNoIndexesEventually();

        synchronizer.onClusterVersionChange(Versions.V3_9);
        assertNoIndexesEventually();
    }

    private void assertNoIndexesEventually() {
        Callable assertion = new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
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
            public Boolean call() throws Exception {
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