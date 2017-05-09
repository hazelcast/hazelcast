package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static java.lang.System.currentTimeMillis;

@Category(SlowTest.class)
public class MapQueryPerfTest extends HazelcastTestSupport {

    @Test
    public void test() {

        // GIVEN
//        Config config = getHDConfig();
        Config config = getConfig();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), String.valueOf(10));

        MapConfig mapConfig = config.getMapConfig("default");
        MapIndexConfig mic = new MapIndexConfig();
        mic.setAttribute("age");
        mic.setOrdered(true);
        mapConfig.addMapIndexConfig(mic);


//        config.getSerializationConfig().addPortableFactory(1, new TestPortableFactory());

        long warmup = 1000 * 10;
        long duration = 1000 * 60;

        HazelcastInstance node = createHazelcastInstance(config);
        IMap map = node.getMap("default");

        for (int i = 0; i < 10000; i++) {
//            TestChildPortable childPortable = new TestChildPortable("i"+i, Long.valueOf(i));
//            TestPortable portable = new TestPortable("i"+i, Long.valueOf(i), childPortable);

            Person person = new Person(String.valueOf(i), i);
            map.put(i, person);
        }

        // WHEN
        long count = run(warmup, map);
        count = run(duration, map);

        // THEN
        System.out.println((count / (duration / 1000)) + " ops/s");

    }

    public long run(long duration, IMap map) {
        long resultCount = 0;
        long count = 0;
        long start = currentTimeMillis();
        while (true) {
            long diff = currentTimeMillis() - start;
            if (diff > duration) {
                break;
            }
            // Collection result = map.values(FalsePredicate.INSTANCE);
            Collection result = map.values(Predicates.equal("age", 10));


            resultCount += result.size();

            count++;
        }
        System.out.println("Result count " + resultCount);
        return count;
    }

}
