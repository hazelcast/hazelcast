/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalBitmapIndexStatsTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{InMemoryFormat.OBJECT}, {InMemoryFormat.BINARY}});
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    protected HazelcastInstance instance;

    protected String mapName;

    protected IMap<Integer, Integer> map;

    protected static final int PARTITIONS = 137;

    @Before
    public void before() {
        mapName = randomMapName();

        Config config = getConfig();
        config.setProperty(PARTITION_COUNT.getName(), Integer.toString(PARTITIONS));
        config.getMapConfig(mapName).setInMemoryFormat(inMemoryFormat);
        config.getMetricsConfig().setEnabled(false);

        instance = createInstance(config);
        map = instance.getMap(mapName);
    }

    @Test
    public void testBitmapStatsPropagated() {
        IMap<Integer, Integer> map = instance.getMap(mapName);
        final int limit = 10;

        map.addIndex(IndexType.HASH, "__key");
        for (int idx = 0; idx < limit; idx++) {
            map.put(idx, idx);
            map.put(idx, idx + 1);
            map.remove(idx);
        }

        assertEquals(1, map.getLocalMapStats().getIndexStats().entrySet().size());
        LocalIndexStats localIndexStats = map.getLocalMapStats().getIndexStats().entrySet().iterator().next().getValue();

        assertEquals("Wrong map insert operations count in hashed index operations loop!", limit, localIndexStats.getInsertCount());
        assertEquals("Wrong map update operations count in hashed index operations loop!", limit, localIndexStats.getUpdateCount());
        assertEquals("Wrong map remove operations count in hashed index operations loop!", limit, localIndexStats.getRemoveCount());

        map.addIndex(IndexType.BITMAP, "__key");
        for (int idx = 0; idx < limit; idx++) {
            map.put(idx, idx);
            map.put(idx, idx + 1);
            map.remove(idx);
        }

        assertEquals(2, map.getLocalMapStats().getIndexStats().entrySet().size());
        for (Map.Entry<String, LocalIndexStats> it : map.getLocalMapStats().getIndexStats().entrySet()) {
            if (it.getKey().contains("bitmap___key")) {
                localIndexStats = it.getValue();
                break;
            }
        }

        assertEquals("Wrong map insert operations count in bitmap index operations loop!", limit, localIndexStats.getInsertCount());
        assertEquals("Wrong map update operations count in bitmap index operations loop!", limit, localIndexStats.getUpdateCount());
        assertEquals("Wrong map remove operations count in bitmap index operations loop!", limit, localIndexStats.getRemoveCount());

    }

    protected HazelcastInstance createInstance(Config config) {
        return createHazelcastInstance(config);
    }

}
