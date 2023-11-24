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

import com.hazelcast.collection.ISet;
import com.hazelcast.config.Config;
import com.hazelcast.config.SetConfig;
import com.hazelcast.internal.namespace.UCDTest;

public abstract class SetUCDTest extends UCDTest {
    protected SetConfig setConfig;
    protected ISet<Object> set;

    @Override
    protected void initialiseConfig() {
        setConfig = new SetConfig(objectName);
        setConfig.setNamespace(getNamespaceName());
    }

    @Override
    protected void initialiseDataStructure() {
        set = instance.getSet(objectName);
    }

    @Override
    protected void registerConfig(Config config) {
        config.addSetConfig(setConfig);
    }

    protected void populate() {
        set.add("item");
    }
}
