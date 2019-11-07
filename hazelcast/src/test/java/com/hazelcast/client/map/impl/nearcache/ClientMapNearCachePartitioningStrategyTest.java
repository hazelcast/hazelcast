/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientMapConfig;
import com.hazelcast.client.map.AbstractClientMapPartitioningStrategyTest;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.PartitioningStrategyConfig;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapNearCachePartitioningStrategyTest extends AbstractClientMapPartitioningStrategyTest {

    @Override
    public ClientConfig getClientConfig() {
        PartitioningStrategyConfig partitioningStrategyConfig
                = new PartitioningStrategyConfig(StringPartitioningStrategy.class.getName());
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.addMapConfig(new ClientMapConfig(mapName)
                .setPartitioningStrategyConfig(partitioningStrategyConfig));
        clientConfig.addNearCacheConfig(new NearCacheConfig(mapName));
        return clientConfig;
    }
}
