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

import com.hazelcast.config.InvalidConfigurationException;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PropertiesToNodeConverterTest {

    @Test(expected = InvalidConfigurationException.class)
    public void shouldThrowWhenParsingEmptyMap() throws Exception {
        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(Collections.emptyMap());
    }

    @Test
    public void shouldParse() {
        Map<String, String> m = new HashMap<>();
        m.put("foo.bar1", "1");
        m.put("foo.bar2", "2");
        m.put("foo.bar3.bar4", "4");
        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(m);

        assertNull(configNode.getValue());
        assertEquals("foo", configNode.getName());
        assertEquals(3, configNode.getChildren().size());
        assertEquals("1", configNode.getChildren().get("bar1").getValue());
        assertEquals("2", configNode.getChildren().get("bar2").getValue());
        assertEquals("4", configNode.getChildren().get("bar3").getChildren().get("bar4").getValue());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void shouldThrowWhenNoSharedRootNode() {
        Map<String, String> m = new HashMap<>();
        m.put("foo1.bar1", "1");
        m.put("foo2.bar2", "2");
        ConfigNode configNode = PropertiesToNodeConverter.propsToNode(m);
    }
}
