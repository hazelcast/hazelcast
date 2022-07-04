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

package com.hazelcast.spi.impl.tenantcontrol;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.SpiDataSerializerHook;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.spi.tenantcontrol.Tenantable;

import javax.annotation.Nonnull;
import java.io.IOException;

/**
 * Default no-op implementation of {@link TenantControl}
 */
public final class NoopTenantControl implements TenantControl, IdentifiedDataSerializable {

    @Override
    public Closeable setTenant() {
        return NoopCloseable.INSTANCE;
    }

    @Override
    public void registerObject(@Nonnull DestroyEventContext destroyEventContext) {
    }

    @Override
    public void unregisterObject() {
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
    }

    @Override
    public void clearThreadContext() {
    }

    @Override
    public boolean isAvailable(@Nonnull Tenantable tenantable) {
        return true;
    }

    private static final class NoopCloseable implements Closeable {

        static final NoopCloseable INSTANCE = new NoopCloseable();

        @Override
        public void close() {
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NoopTenantControl;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.NOOP_TENANT_CONTROL;
    }
}
