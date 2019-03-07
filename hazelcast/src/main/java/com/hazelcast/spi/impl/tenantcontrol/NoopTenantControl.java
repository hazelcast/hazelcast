/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.tenantcontrol.TenantControl;

import java.io.Closeable;

/**
 * Default no-op implementation of TenantControl
 */
@SerializableByConvention
public final class NoopTenantControl implements TenantControl {

    @Override
    public Closeable setTenant(boolean createRequestScope) {
        return NoopCloseable.INSTANCE;
    }

    @Override
    public void unregister() {
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
}
