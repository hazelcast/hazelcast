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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.services.NodeAware;
import com.hazelcast.internal.serialization.SerializationServiceAware;

public final class HazelcastManagedContext implements ManagedContext {

    private final HazelcastInstanceImpl instance;
    private final ManagedContext externalContext;
    private final boolean hasExternalContext;

    public HazelcastManagedContext(HazelcastInstanceImpl instance, ManagedContext externalContext) {
        this.instance = instance;
        this.externalContext = externalContext;
        this.hasExternalContext = externalContext != null;
    }

    @Override
    public Object initialize(Object obj) {
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(instance);
        }
        if (obj instanceof NodeAware) {
            ((NodeAware) obj).setNode(instance.node);
        }
        if (obj instanceof SerializationServiceAware) {
            ((SerializationServiceAware) obj).setSerializationService(instance.node.getSerializationService());
        }

        if (hasExternalContext) {
            return externalContext.initialize(obj);
        }
        return obj;
    }
}
