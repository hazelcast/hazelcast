/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

import static com.hazelcast.internal.config.override.SystemPropertiesConfigParser.member;
import static org.junit.Assert.assertNull;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SystemPropertiesConfigProviderTest {
    private Properties systemProperties;
    private SystemPropertiesConfigProvider provider;

    @Before
    public void setUp() {
        systemProperties = new Properties();
        provider = new SystemPropertiesConfigProvider(member(), () -> systemProperties);
    }

    @Test
    public void shouldParseClusternameConfigFromSystemProperties() {
        systemProperties.put("hz.cluster-name", "testcluster");
        assertThat(provider.properties()).containsEntry("hazelcast.cluster-name", "testcluster");
    }

    /** @see <a href="https://github.com/hazelcast/hazelcast/issues/26310">GitHub issue</a> */
    @Test
    public void testNonStringSystemPropertyValue() {
        String key = getClass().getSimpleName();
        systemProperties.put(key, true);
        assertNull(provider.properties().get(key));
    }
}
