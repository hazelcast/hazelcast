/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.impl.FactoryImpl;

/**
 * @mdogan 4/6/12
 */
public final class HazelcastManagedContext implements ManagedContext {

    private final FactoryImpl factory;
    private final ManagedContext externalContext;
    private final boolean hasExternalContext;

    public HazelcastManagedContext(final FactoryImpl factory, final ManagedContext externalContext) {
        this.factory = factory;
        this.externalContext = externalContext;
        hasExternalContext = this.externalContext != null;
    }

    public final void initialize(final Object obj) {
        if (obj instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) obj).setHazelcastInstance(factory);
        }

        if (hasExternalContext) {
            externalContext.initialize(obj);
        }
    }
}
