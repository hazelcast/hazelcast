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

import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.spi.tenantcontrol.TenantControlFactory;

import static com.hazelcast.spi.tenantcontrol.TenantControl.NOOP_TENANT_CONTROL;

/**
 * Default implementation of {@link TenantControlFactory}, always returns a no-op tenant control
 */
public class NoopTenantControlFactory implements TenantControlFactory {

    @Override
    public TenantControl saveCurrentTenant() {
        return NOOP_TENANT_CONTROL;
    }

    @Override
    public boolean isClassesAlwaysAvailable() {
        return true;
    }
}
