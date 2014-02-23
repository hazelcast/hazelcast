/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.instance;

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.spi.NodeAware;

/**
 * @author mdogan 4/6/12
 */
public final class HazelcastManagedContext implements ManagedContext {

    private final HazelcastInstanceImpl instance;
    private final ManagedContext externalContext;
    private final boolean hasExternalContext;

    public HazelcastManagedContext(final HazelcastInstanceImpl instance, final ManagedContext externalContext) {
        this.instance = instance;
        this.externalContext = externalContext;
        hasExternalContext = this.externalContext != null;
    }

    public final Object initialize(Object obj) {
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(instance);
        }
        if (obj instanceof NodeAware) {
            ((NodeAware) obj).setNode(instance.node);
        }

        if (hasExternalContext) {
            obj = externalContext.initialize(obj);
        }
        return obj;
    }
}
