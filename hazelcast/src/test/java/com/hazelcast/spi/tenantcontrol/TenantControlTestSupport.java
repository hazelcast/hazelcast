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

package com.hazelcast.spi.tenantcontrol;

import com.hazelcast.config.Config;
import com.hazelcast.internal.util.AdditionalServiceClassLoader;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.test.HazelcastTestSupport;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class TenantControlTestSupport extends HazelcastTestSupport {
    protected static final ThreadLocal<TenantControl> savedTenant = new ThreadLocal<>();
    protected static final AtomicBoolean tenantFactoryInitialized = new AtomicBoolean();
    protected static final AtomicInteger setTenantCount = new AtomicInteger();
    protected static final AtomicInteger closeTenantCount = new AtomicInteger();
    protected static final AtomicInteger registerTenantCount = new AtomicInteger();
    protected static final AtomicInteger unregisterTenantCount = new AtomicInteger();
    protected static final AtomicInteger clearedThreadInfoCount = new AtomicInteger();
    protected static final AtomicInteger tenantAvailableCount = new AtomicInteger();
    protected static final AtomicBoolean tenantAvailable = new AtomicBoolean();
    protected static final AtomicReference<DestroyEventContext> destroyEventContext = new AtomicReference<>(null);

    protected static volatile boolean classesAlwaysAvailable;

    protected void initState() {
        tenantFactoryInitialized.set(false);
        savedTenant.remove();
        setTenantCount.set(0);
        closeTenantCount.set(0);
        registerTenantCount.set(0);
        unregisterTenantCount.set(0);
        clearedThreadInfoCount.set(0);
        tenantAvailable.set(true);
        classesAlwaysAvailable = false;
    }

    protected Config newConfig() {
        return newConfig(true, TenantControlTest.class.getClassLoader());
    }

    protected Config newConfig(boolean hasTenantControl, ClassLoader testClassLoader) {
        Config config = smallInstanceConfig();
        if (hasTenantControl) {
            ClassLoader configClassLoader = new AdditionalServiceClassLoader(new URL[0],
                    testClassLoader);
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
            return false;
        }
    }

    public static class CountingTenantControl implements TenantControl {
        private final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, MILLISECONDS.toNanos(1), MILLISECONDS.toNanos(100));
        private int idleCount = 0;

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
            tenantAvailableCount.incrementAndGet();
            boolean available = tenantAvailable.get();
            if (!available) {
                idleStrategy.idle(idleCount++);
            } else {
                idleCount = 0;
            }
            return available;
        }

        @Override
        public void clearThreadContext() {
            clearedThreadInfoCount.incrementAndGet();
        }
    }
}
