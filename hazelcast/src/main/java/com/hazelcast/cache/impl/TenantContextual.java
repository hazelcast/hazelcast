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

package com.hazelcast.cache.impl;

import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.util.ExceptionUtil;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Represents a value that requires tenant control context to be accessed
 *
 * @author lprimak
 * @param <Factory> object type
 */
public class TenantContextual<Factory> {
    private Factory factory;
    private final Callable<Factory> initFunction;
    private final Callable<Boolean> existsFunction;
    private final TenantControl tenantControl;
    private boolean initialized;

    public TenantContextual(Callable<Factory> initFunction, Callable<Boolean> existsFunction, TenantControl tenantControl) {
        this.initFunction = initFunction;
        this.existsFunction = existsFunction;
        this.tenantControl = tenantControl;
    }

    /**
     *
     * @return underlying factory, within Tenant Control
     */
    public Factory get() {
        if (!initialized) {
            Closeable tenantContext = null;
            try {
                if (exists()) {
                    tenantContext = tenantControl.setTenant(true);
                    factory = initFunction.call();
                }
                initialized = true;
            } catch (Exception ex) {
                ExceptionUtil.rethrow(ex);
            } finally {
                try {
                    if (tenantContext != null) {
                        tenantContext.close();
                    }
                } catch (IOException ex) {
                    ExceptionUtil.rethrow(ex);
                }
            }
        }
        return factory;
    }

    /**
     * could be called isNull() for the underlying object
     *
     * @return true if the underlying object exists (it not null)
     */
    public Boolean exists() {
        try {
            return existsFunction.call();
        } catch (Exception ex) {
            ExceptionUtil.rethrow(ex);
        }
        return false;
    }

    /**
     * method to return delegate having same contextual and creation methods
     * as the current class
     *
     * @param delegate
     * @return newly-created delegate
     */
    public TenantContextual<Factory> delegate(Factory delegate) {
        TenantContextual<Factory> contextual = new TenantContextual(initFunction, existsFunction, tenantControl);
        contextual.initialized = true;
        contextual.factory = delegate;
        return contextual;
    }
}
