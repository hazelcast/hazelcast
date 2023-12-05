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

package com.hazelcast.internal.namespace.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.internal.namespace.UCDTest;
import com.hazelcast.replicatedmap.ReplicatedMap;
import org.apache.commons.lang3.stream.Streams;
import org.junit.runners.Parameterized;

import java.util.stream.Collectors;

public abstract class ReplicatedMapUCDTest extends UCDTest {
    protected ReplicatedMapConfig replicatedMapConfig;
    protected ReplicatedMap<Object, Object> map;

    @Override
    protected void initialiseConfig() {
        replicatedMapConfig = new ReplicatedMapConfig(objectName);
        replicatedMapConfig.setNamespace(getNamespaceName());
    }

    @Override
    protected void initialiseDataStructure() {
        map = instance.getReplicatedMap(objectName);
    }

    protected void populate() {
        map.put(1, 1);
    }

    @Override
    protected void registerConfig(Config config) {
        config.addReplicatedMapConfig(replicatedMapConfig);
    }

    @Parameterized.Parameters(name = "Connection: {0}, Config: {1}, Class Registration: {2}, Assertion: {3}")
    public static Iterable<Object[]> parameters() {
        // Skip MEMBER_TO_MEMBER ConnectionTypes because ReplicatedMaps cannot be created on Lite members
        return Streams.of(UCDTest.parameters())
                .filter(obj -> obj[0] != ConnectionStyle.MEMBER_TO_MEMBER)
                .collect(Collectors.toList());
    }
}
