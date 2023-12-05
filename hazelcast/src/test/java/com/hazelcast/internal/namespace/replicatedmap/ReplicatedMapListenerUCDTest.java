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

import com.hazelcast.config.EntryListenerConfig;
import org.junit.runners.Parameterized;

import java.util.stream.Collectors;

public abstract class ReplicatedMapListenerUCDTest extends ReplicatedMapUCDTest {
    @Override
    protected void addClassInstanceToConfig() throws ReflectiveOperationException {
        EntryListenerConfig entryListenerConfig = new EntryListenerConfig();
        entryListenerConfig.setImplementation(getClassInstance());
        replicatedMapConfig.addEntryListenerConfig(entryListenerConfig);
    }

    @Override
    protected void addClassNameToConfig() {
        EntryListenerConfig listenerConfig = new EntryListenerConfig();
        listenerConfig.setClassName(getUserDefinedClassName());
        replicatedMapConfig.addEntryListenerConfig(listenerConfig);
    }

    @Override
    protected void addClassInstanceToDataStructure() throws ReflectiveOperationException {
        map.addEntryListener(getClassInstance());
    }

    @Parameterized.Parameters(name = "Connection: {0}, Config: {1}, Class Registration: {2}, Assertion: {3}")
    public static Iterable<Object[]> parameters() {
        // Skip MEMBER_TO_MEMBER ConnectionTypes because ReplicatedMaps cannot be created on Lite members
        return listenerParameters()
                .stream()
                .filter(obj -> obj[0] != ConnectionStyle.MEMBER_TO_MEMBER)
                .collect(Collectors.toList());
    }

    @Override
    protected String getUserDefinedClassName() {
        return "usercodedeployment.MyEntryListener";
    }
}
