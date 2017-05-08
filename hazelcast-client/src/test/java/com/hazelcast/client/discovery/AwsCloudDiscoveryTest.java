/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.discovery;

import com.hazelcast.aws.AwsDiscoveryStrategyFactory;
import com.hazelcast.aws.AwsProperties;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

@RunWith(HazelcastParallelClassRunner.class)
@Category({SlowTest.class, ParallelTest.class})
public class AwsCloudDiscoveryTest {
    @Test
    public void testAwsClient_MemberNonDefaultPortConfig() {
        final Map<String, Comparable> props = new HashMap<String, Comparable>();
        props.put(AwsProperties.PORT.getDefinition().key(), "60000");
        props.put(AwsProperties.ACCESS_KEY.getDefinition().key(), System.getenv("AWS_ACCESS_KEY_ID"));
        props.put(AwsProperties.SECRET_KEY.getDefinition().key(), System.getenv("AWS_SECRET_ACCESS_KEY"));
        props.put(AwsProperties.TAG_KEY.getDefinition().key(), "aws-test-tag");
        props.put(AwsProperties.TAG_VALUE.getDefinition().key(), "aws-tag-value-1");
        props.put(AwsProperties.CONNECTION_TIMEOUT_SECONDS.getDefinition().key(), "10");

        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().getDiscoveryConfig()
              .addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(new AwsDiscoveryStrategyFactory(), props));

        config.setProperty(ClientProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        config.setProperty(ClientProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED.getName(), "true");

        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap<Object, Object> map = client.getMap("MyMap");
        map.put(1, 5);
        Assert.assertEquals(5, map.get(1));
    }
}
