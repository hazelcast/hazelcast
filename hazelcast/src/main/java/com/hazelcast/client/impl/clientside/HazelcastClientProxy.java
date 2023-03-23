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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.impl.AbstractHazelcastInstanceProxy;
import com.hazelcast.instance.impl.TerminatedLifecycleService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.SerializationServiceSupport;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionService;

import javax.annotation.Nonnull;

/**
 * A client-side proxy {@link com.hazelcast.core.HazelcastInstance} instance.
 */
public class HazelcastClientProxy extends AbstractHazelcastInstanceProxy<HazelcastClientInstanceImpl>
        implements HazelcastInstance, SerializationServiceSupport {

    public HazelcastClientProxy(HazelcastClientInstanceImpl target) {
        super(target);
    }

    @Nonnull
    @Override
    public SplitBrainProtectionService getSplitBrainProtectionService() {
        throw new UnsupportedOperationException();
    }

    @Nonnull
    @Override
    public LifecycleService getLifecycleService() {
        final HazelcastClientInstanceImpl hz = target;
        return hz != null ? hz.getLifecycleService() : new TerminatedLifecycleService();
    }

    public ClientConfig getClientConfig() {
        return getTarget().getClientConfig();
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
    public HazelcastClientInstanceImpl getTarget() {
        final HazelcastClientInstanceImpl c = target;
        if (c == null || !c.getLifecycleService().isRunning()) {
            throw new HazelcastClientNotActiveException();
        }
        return c;
    }

    public String toString() {
        final HazelcastClientInstanceImpl hazelcastInstance = target;
        if (hazelcastInstance != null) {
            return hazelcastInstance.toString();
        }
        return "HazelcastClientInstance {NOT ACTIVE}";
    }
}
