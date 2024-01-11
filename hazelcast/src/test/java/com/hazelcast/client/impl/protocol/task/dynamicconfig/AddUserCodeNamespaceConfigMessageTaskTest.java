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

package com.hazelcast.client.impl.protocol.task.dynamicconfig;

import com.hazelcast.client.impl.protocol.codec.DynamicConfigAddUserCodeNamespaceConfigCodec;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.UserCodeNamespaceConfig;
import org.junit.Test;

import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class AddUserCodeNamespaceConfigMessageTaskTest
        extends ConfigMessageTaskTest {
    @Test
    public void test() {
        UserCodeNamespaceConfig config = new UserCodeNamespaceConfig("my-namespace");


        AddUserCodeNamespaceConfigMessageTask task = new AddUserCodeNamespaceConfigMessageTask(
                DynamicConfigAddUserCodeNamespaceConfigCodec.encodeRequest(config.getName(),
                        ConfigAccessor.getResourceDefinitions(config)
                                      .stream()
                                      .map(ResourceDefinitionHolder::new)
                                      .collect(Collectors.toList())),
                mockNode, mockConnection);
        task.run();

        assertEquals(config, task.getConfig());
    }
}
