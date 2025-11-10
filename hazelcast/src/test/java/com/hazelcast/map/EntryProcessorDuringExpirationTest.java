/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Pipelining;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for entry processors executed when entries are concurrently expiring during EP execution
 */
@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category({SlowTest.class, ParallelJVMTest.class})
public class EntryProcessorDuringExpirationTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameterized.Parameter(1)
    public boolean forceOffload;

    @Parameterized.Parameters(name = "inMemoryFormat: {0}, forceOffload: {1}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY, false},
                {OBJECT, false},
                {OBJECT, true}
        });
    }

    private final String mapName = "ep-test-map";

    @Override
    public Config getConfig() {
        // use regular instance to increase likelihood of expiration during operation
        // with more partitions and more partition threads
        Config config = regularInstanceConfig();
        config.getMetricsConfig().setEnabled(false);
        config.setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(),
                String.valueOf(forceOffload));
        config.setProperty(ClusterProperty.PARTITION_OPERATION_THREAD_COUNT.getName(), "16")
                // we want the expired entries to be found by iteration
            .setProperty(MapClearExpiredRecordsTask.PROP_CLEANUP_ENABLED, "false");

        config.getMapConfig(mapName)
                .setInMemoryFormat(inMemoryFormat)
                .setPerEntryStatsEnabled(true);
        return config;
    }

    /**
     * Without fixes in HD this test does not fail always (but sometimes does) due to operation retries
     * in case of NPE. But even if it passes, it generates NPE in the logs.
     */
    @Test
    public void removeAllDuringExpiration() throws Exception {
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(getConfig(), 2);
        var instance1 = hazelcastInstances[0];

        var map = fillMap(instance1);

        // removeAll uses entry processor under the hood
        map.removeAll(Predicates.alwaysTrue());

        // map size may include entries that expired but were not yet removed, so cannot be used in assertions
        assertThat(map.entrySet()).isEmpty();
    }

    @Test
    public void entryProcessorInvocationDuringExpiration() throws Exception {
        HazelcastInstance[] hazelcastInstances = createHazelcastInstances(getConfig(), 2);
        var instance1 = hazelcastInstances[0];

        IMap<Long, String> map = fillMap(instance1);

        map.executeOnEntries(e -> {
            if (e.getKey() % 1000 == 0) {
                // this entry processor is relatively slow, it can cause backup timeouts
                // but the data should be fine.
                sleepMillis(100);
            }
            if (e.getValue().length() == 20) {
                e.setValue("123");
            } else {
                // this indicates retried invocation on the same entry.
                // while entry processor in general are better if they are idempotent,
                // but in this test there are no conditions that should trigger the retry
                // by design (member crash, migration).
                e.setValue(e.getValue() + "123");
            }
            return null;
        });

        assertThat((Map<Long, String>) map).values().containsOnly("123");
    }


    private IMap<Long, String> fillMap(HazelcastInstance instance) throws Exception {
        IMap<Long, String> map = instance.getMap(mapName);
        Pipelining<Void> p = new Pipelining<>(100);
        for (long i = 0; i < 1_000_000; i++) {
            // this does not require physical eviction (it is disabled by options)
            // half of the entries should expire during execution
            p.add(map.setAsync(i, randomAlphabetic(20), i % 2 == 0 ? 3 : 10000, TimeUnit.SECONDS));
        }
        p.results();
        return map;
    }
}
