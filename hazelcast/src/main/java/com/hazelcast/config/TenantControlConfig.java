/*
 * Copyright 2018 Hazelcast, Inc..
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.config;

import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;

/**
 * Configuration for Tenant Control
 * This is used for application servers or other multi-tenant architectures
 * to associate a particular application with it's context, such as
 * thread-local variable, class loaders, etc.
 *
 * className property instantiates the implementation class
 * Defaults to NoTenantControl
 *
 * @author lprimak
 */
public final class TenantControlConfig {
    /**
     * @return class name of implementation
     */
    public String getClassName() {
        return implementation.getClass().getName();
    }

    /**
     * Instantiates and sets the implementation as per the className argument
     * If classLoader is set, it is used to instantiate the class
     *
     * @param className
     * @return this
     */
    public TenantControlConfig setClassName(String className) {
        if(className == null) {
            throw new IllegalArgumentException("TenantControlConfig.setClassName(String className) - className cannot be null");
        }

        ClassLoader cl = this.classLoader != null? this.classLoader : Thread.currentThread().getContextClassLoader();
        try {
            implementation = ClassLoaderUtil.newInstance(cl, className);
        } catch (Exception ex) {
            Logger.getLogger(TenantControlConfig.class).warning("Unable to set Tenant Implementation", ex);
        }
        return this;
    }

    /**
     * @return current implementation of tenant control
     */
    public TenantControl getImplementation() {
        return implementation;
    }

    /**
     * Sets the implementation of tenant control
     *
     * @param implementation
     * @return this
     */
    public TenantControlConfig setImplementation(TenantControl implementation) {
        this.implementation = implementation;
        return this;
    }

    /**
     * @return current class loader for configuration or null
     */
    public ClassLoader getClassLoader() {
        return classLoader;
    }

    /**
     * Sets class loader for instantiation of tenant control
     *
     * @param classLoader
     * @return this
     */
    public TenantControlConfig setClassLoader(ClassLoader classLoader) {
        this.classLoader = classLoader;
        return this;
    }

    @Override
    public String toString() {
        return "TenantControlConfig{" + "implementation=" + implementation + '}';
    }


    private TenantControl implementation = new TenantControl.NoTenantControl();
    private ClassLoader classLoader;
}
