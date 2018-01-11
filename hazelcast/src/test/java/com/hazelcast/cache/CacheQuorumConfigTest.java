/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.quorum.QuorumType;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class CacheQuorumConfigTest extends HazelcastTestSupport {

    private final URL configUrl = getClass().getClassLoader().getResource("test-hazelcast-jcache-with-quorum.xml");

    @Test
    public void cacheConfigXmlTest() throws IOException {
        final String cacheName = "configtestCache" + randomString();
        Config config = new XmlConfigBuilder(configUrl).build();
        CacheSimpleConfig cacheConfig1 = config.getCacheConfig(cacheName);
        final String quorumName = cacheConfig1.getQuorumName();

        assertEquals("cache-quorum", quorumName);

        QuorumConfig quorumConfig = config.getQuorumConfig(quorumName);
        assertEquals(3, quorumConfig.getSize());
        assertEquals(QuorumType.READ_WRITE, quorumConfig.getType());
    }
}
