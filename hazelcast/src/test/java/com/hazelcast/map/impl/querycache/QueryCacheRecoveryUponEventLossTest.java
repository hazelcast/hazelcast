package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.EventLostEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.subscriber.TestSubscriberContext;
import com.hazelcast.map.listener.EventLostListener;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheRecoveryUponEventLossTest extends HazelcastTestSupport {

    @Test
    public void testForceConsistency() {
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(3);

        String mapName = randomString();
        String queryCacheName = randomString();

        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(queryCacheName);
        queryCacheConfig.setBatchSize(1111);
        queryCacheConfig.setDelaySeconds(3);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.addQueryCacheConfig(queryCacheConfig);
        mapConfig.setBackupCount(0);

        HazelcastInstance node = instanceFactory.newHazelcastInstance(config);
        HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        setTestSequencer(node, 9);
        setTestSequencer(node2, 9);

        IMap<Integer, Integer> map = getMap(node, mapName);
        node2.getMap(mapName);

        //set test sequencer to subscribers.
        int count = 30;

        final QueryCache queryCache = map.getQueryCache(queryCacheName, new SqlPredicate("this > 20"), true);
        queryCache.addEntryListener(new EventLostListener() {
            @Override
            public void eventLost(EventLostEvent event) {
                queryCache.tryRecover();

            }
        }, false);

        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(9, queryCache.size());
            }
        };

        assertTrueEventually(task);
    }

    private void setTestSequencer(HazelcastInstance instance, int eventCount) {
        Node node = getNode(instance);
        MapService service = node.getNodeEngine().getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        queryCacheContext.setSubscriberContext(new TestSubscriberContext(queryCacheContext, eventCount, true));
    }
}
