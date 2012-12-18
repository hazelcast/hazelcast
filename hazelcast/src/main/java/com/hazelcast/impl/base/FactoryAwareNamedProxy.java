/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl.base;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceAwareInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;

public abstract class FactoryAwareNamedProxy implements HazelcastInstanceAwareInstance {
    transient protected HazelcastInstanceImpl hazelcastInstance = null;
    protected String name = null;

    protected FactoryAwareNamedProxy() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = (HazelcastInstanceImpl) hazelcastInstance;
    }
}
