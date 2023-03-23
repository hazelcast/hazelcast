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

package com.hazelcast.instance.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.SerializationServiceSupport;

import javax.annotation.Nonnull;

/**
 * A proxy around the actual HazelcastInstanceImpl. This class serves 2 purposes:
 * <ol>
 * <li>
 * if the HazelcastInstance is shut down, the reference to the original HazelcastInstanceImpl is nulled and
 * this HazelcastInstanceImpl and all its dependencies can be GCed. If the HazelcastInstanceImpl would
 * be exposed directly, it could still retain unusable objects due to its not-null fields.</li>
 * <li>
 * it provides a barrier for accessing the HazelcastInstanceImpl internals. Otherwise a simple cast to HazelcastInstanceImpl
 * would be sufficient but now a bit of reflection is needed to get there.
 * </li>
 * </ol>
 */
public final class HazelcastInstanceProxy
        extends AbstractHazelcastInstanceProxy<HazelcastInstanceImpl>
        implements HazelcastInstance, SerializationServiceSupport {

    private final String name;

    protected HazelcastInstanceProxy(HazelcastInstanceImpl target) {
        super(target);
        name = target.getName();
    }

    @Nonnull
    @Override
    public String getName() {
        return name;
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        final HazelcastInstanceImpl hz = target;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    @Override
    public void shutdown() {
        getLifecycleService().shutdown();
    }

    @Override
    public InternalSerializationService getSerializationService() {
        return getTarget().getSerializationService();
    }

    @Override
    public String toString() {
        final HazelcastInstanceImpl hazelcastInstance = target;
        if (hazelcastInstance != null) {
            return hazelcastInstance.toString();
        }
        return "HazelcastInstance {NOT ACTIVE}";
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof HazelcastInstance)) {
            return false;
        }

        HazelcastInstance that = (HazelcastInstance) o;
        return !(name != null ? !name.equals(that.getName()) : that.getName() != null);
    }
}


