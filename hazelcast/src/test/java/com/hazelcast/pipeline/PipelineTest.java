package com.hazelcast.pipeline;

import com.hazelcast.cache.ICache;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.util.concurrent.Pipe;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class PipelineTest extends HazelcastTestSupport {

    private HazelcastInstance hz;

    @Before
    public void before() {
        hz = createHazelcastInstance();
    }

    @Test
    public void testAtomicLong() throws Exception {
        Pipeline<Long> pipeline = hz.newPipeline(100);
        for (int k = 0; k < 100000; k++) {
            pipeline.add(hz.getAtomicLong("" + k % 100).getAsync());
        }

        System.out.println(pipeline.results());
    }

    @Test
    public void testMapGet() throws Exception {
        IMap<String, String> map = hz.getMap("foo");
        Pipeline<String> pipeline = hz.newPipeline(100);
        for (int k = 0; k < 100000; k++) {
            pipeline.add(map.getAsync("" + k));
        }

        System.out.println(pipeline.results());
    }
    @Test
    public void testMapPut() throws Exception {
        IMap<String, String> map = hz.getMap("foo");
        Pipeline<String> pipeline = hz.newPipeline(100);
        for (int k = 0; k < 100000; k++) {
            pipeline.add(map.putAsync("" + k,"foobar"));
        }

        System.out.println(pipeline.results());
    }

    @Test
    public void testCacheGet() throws Exception {
        ICache<String, String> map = hz.getCacheManager().getCache("foo");
        Pipeline<String> pipeline = hz.newPipeline(100);
        for (int k = 0; k < 100000; k++) {
            pipeline.add(map.getAsync("" + k));
        }

        System.out.println(pipeline.results());
    }
}
