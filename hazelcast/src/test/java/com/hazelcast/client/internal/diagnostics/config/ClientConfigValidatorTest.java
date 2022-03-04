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

package com.hazelcast.client.internal.diagnostics.config;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.config.NearCacheConfig.LocalUpdatePolicy.CACHE_ON_UPDATE;
import static org.hamcrest.CoreMatchers.containsString;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientConfigValidatorTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "default";

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Test
    public void getMap_throws_illegalArgumentException_whenLocalUpdatePolicy_is_cacheOnUpdate() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setLocalUpdatePolicy(CACHE_ON_UPDATE);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        thrown.expect(InvalidConfigurationException.class);
        thrown.expectMessage(containsString("Wrong `local-update-policy`"));

        client.getMap(MAP_NAME);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }
}
