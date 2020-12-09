/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.tenantcontrol;

import com.hazelcast.config.Config;
import com.hazelcast.internal.util.AdditionalServiceClassLoader;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastTestSupport;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public abstract class TenantControlTestSupport extends HazelcastTestSupport {
    protected static final ThreadLocal<TenantControl> savedTenant = new ThreadLocal<>();
    protected static final AtomicBoolean tenantFactoryInitialized = new AtomicBoolean();
    protected static final AtomicInteger setTenantCount = new AtomicInteger();
    protected static final AtomicInteger closeTenantCount = new AtomicInteger();
    protected static final AtomicInteger registerTenantCount = new AtomicInteger();
    protected static final AtomicInteger unregisterTenantCount = new AtomicInteger();
    protected static final AtomicInteger clearedThreadInfoCount = new AtomicInteger();
    protected static final AtomicReference<DestroyEventContext> destroyEventContext
            = new AtomicReference<>(null);

    protected static volatile boolean classesAlwaysAvailable;

    protected void initState() {
        tenantFactoryInitialized.set(false);
        savedTenant.remove();
        setTenantCount.set(0);
        closeTenantCount.set(0);
        registerTenantCount.set(0);
        unregisterTenantCount.set(0);
        clearedThreadInfoCount.set(0);
        classesAlwaysAvailable = false;
    }

    protected Config newConfig() {
        return newConfig(true);
    }

    protected Config newConfig(boolean hasTenantControl) {
        Config config = smallInstanceConfig();
        if (hasTenantControl) {
            ClassLoader configClassLoader = new AdditionalServiceClassLoader(new URL[0],
                    TenantControlTest.class.getClassLoader());
            config.setClassLoader(configClassLoader);
        }
        config.getCacheConfig("*");
        return config;
    }

    public static class CountingTenantControlFactory implements TenantControlFactory {
        @Override
        public TenantControl saveCurrentTenant() {
            if (tenantFactoryInitialized.compareAndSet(false, true)) {
                TenantControl tenantControl;
                if (savedTenant.get() == null) {
                    tenantControl = new CountingTenantControl();
                    savedTenant.set(tenantControl);
                } else {
                    tenantControl = savedTenant.get();
                }
                return tenantControl;
            } else if (savedTenant.get() != null) {
                return savedTenant.get();
            } else {
                return TenantControl.NOOP_TENANT_CONTROL;
            }
        }

        @Override
        public boolean isClassesAlwaysAvailable() {
            return classesAlwaysAvailable;
        }
    }

    public static class CountingTenantControl implements TenantControl {
        @Override
        public Closeable setTenant() {
            if (!isAvailable(null)) {
                throw new IllegalStateException("Tenant Not Available");
            }
            setTenantCount.incrementAndGet();
            return closeTenantCount::incrementAndGet;
        }

        @Override
        public void registerObject(@Nonnull DestroyEventContext event) {
            destroyEventContext.set(event);
            registerTenantCount.incrementAndGet();
        }

        @Override
        public void unregisterObject() {
            unregisterTenantCount.incrementAndGet();
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        @Override
        public boolean isAvailable(@Nonnull Tenantable tenantable) {
            return true;
        }

        @Override
        public void clearThreadContext() {
            clearedThreadInfoCount.incrementAndGet();
        }
    }
}
