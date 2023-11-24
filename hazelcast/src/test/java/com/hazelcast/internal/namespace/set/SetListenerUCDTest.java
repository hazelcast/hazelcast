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

package com.hazelcast.internal.namespace.set;

import com.hazelcast.config.ItemListenerConfig;
import org.junit.runners.Parameterized;

public abstract class SetListenerUCDTest extends SetUCDTest {
    @Override
    protected void addClassInstanceToConfig() throws ReflectiveOperationException {
        ItemListenerConfig itemListenerConfig = new ItemListenerConfig();
        itemListenerConfig.setImplementation(getClassInstance());
        setConfig.addItemListenerConfig(itemListenerConfig);
    }

    @Override
    protected void addClassNameToConfig() {
        ItemListenerConfig itemListenerConfig = new ItemListenerConfig();
        itemListenerConfig.setClassName(getUserDefinedClassName());
        setConfig.addItemListenerConfig(itemListenerConfig);
    }

    @Override
    protected void addClassInstanceToDataStructure() throws ReflectiveOperationException {
        set.addItemListener(getClassInstance(), false);
    }

    @Parameterized.Parameters(name = "Connection: {0}, Config: {1}, Class Registration: {2}, Assertion: {3}")
    public static Iterable<Object[]> parameters() {
        return listenerParameters();
    }

    @Override
    protected String getUserDefinedClassName() {
        return "usercodedeployment.MyItemListener";
    }
}
