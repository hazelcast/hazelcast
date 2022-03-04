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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(SlowTest.class)
public class ClientMapNearCacheSlowTest extends ClientMapNearCacheTest {

    @Parameters(name = "batchInvalidationEnabled:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[]{TRUE}, new Object[]{FALSE});
    }

    @Parameter
    public boolean batchInvalidationEnabled;

    @Override
    protected Config newConfig() {
        return getBaseConfig()
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), String.valueOf(batchInvalidationEnabled));
    }
}
