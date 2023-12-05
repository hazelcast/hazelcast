/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddNamespaceConfigCodec;
import com.hazelcast.config.NamespaceConfig;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AddNamespaceConfigMessageTaskTest extends ConfigMessageTaskTest {
    @Test
    public void test() {
        NamespaceConfig config = new NamespaceConfig("my-namepsace");

        AddNamespaceConfigMessageTask task = new AddNamespaceConfigMessageTask(
                DynamicConfigAddNamespaceConfigCodec.encodeRequest(config.getName(),
                        config.getResourceConfigs().stream().map(ResourceDefinitionHolder::new).collect(Collectors.toList())),
                mockNode, mockConnection);
        task.run();

        assertEquals(config, task.getConfig());
    }
}
