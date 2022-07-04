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

package com.hazelcast.internal.config.override;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SystemPropertiesMemberConfigParserTest extends HazelcastTestSupport {

    private final SystemPropertiesConfigParser envVariablesConfigParser = SystemPropertiesConfigParser.member();

    @Test
    public void shouldParseProperties() {
        Properties props = new Properties();
        props.put("hz.cluster-name", "foo");
        props.put("hz.network.join.tcp-ip.enabled", "false");
        props.put("hz.network.join.multicast.enabled", "false");

        Map<String, String> result = envVariablesConfigParser.parse(props);

        assertContains(result.entrySet(), entry("hazelcast.cluster-name", "foo"));
        assertContains(result.entrySet(), entry("hazelcast.network.join.tcp-ip.enabled", "false"));
        assertContains(result.entrySet(), entry("hazelcast.network.join.multicast.enabled", "false"));
    }

    @Test
    public void shouldIgnoreEntriesWithWrongPrefixes() {
        Properties props = new Properties();
        props.put("hz_cluster-name", "foo");
        props.put("hz_network.join.tcp-ip.enabled", "false");
        props.put("hz_network.join.multicast.enabled", "false");

        Map<String, String> result = envVariablesConfigParser.parse(props);

        assertEquals(0, result.size());
    }

    private static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }
}
