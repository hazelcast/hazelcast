/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.config;

import com.hazelcast.client.spi.ClientProxyFactory;

/**
 * This class is related to SPI. To register custom services to java client.
 */
public class ProxyFactoryConfig {

    private String service;

    private String className;

    private ClientProxyFactory factoryImpl;

    public ProxyFactoryConfig() {
    }

    public ProxyFactoryConfig(String className, String service) {
        this.className = className;
        this.service = service;
    }

    public ProxyFactoryConfig(String service, ClientProxyFactory factoryImpl) {
        this.service = service;
        this.factoryImpl = factoryImpl;
    }

    /**
     * @return class name of proxy factory
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets class name of proxy factory
     *
     * @param className of proxy factory
     */
    public ProxyFactoryConfig setClassName(String className) {
        this.className = className;
        return this;
    }

    /**
     * @return service name of related proxy factory
     */
    public String getService() {
        return service;
    }

    /**
     * @param service for given proxy factory
     */
    public ProxyFactoryConfig setService(String service) {
        this.service = service;
        return this;
    }

    /**
     * @return implementation of proxy factory
     */
    public ClientProxyFactory getFactoryImpl() {
        return factoryImpl;
    }

    /**
     * Sets factory implementation of proxy factory
     *
     * @param factoryImpl of proxy factory
     */
    public ProxyFactoryConfig setFactoryImpl(ClientProxyFactory factoryImpl) {
        this.factoryImpl = factoryImpl;
        return this;
    }

    private String internalClassName() {
        if (factoryImpl != null) {
            return factoryImpl.getClass().getName();
        }
        return className;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProxyFactoryConfig that = (ProxyFactoryConfig) o;

        String thisClassName = internalClassName();
        String thatClassName = that.internalClassName();
        if (thisClassName != null ? !thisClassName.equals(thatClassName) : thatClassName != null) {
            return false;
        }
        if (service != null ? !service.equals(that.service) : that.service != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = service != null ? service.hashCode() : 0;
        String internalClassName = internalClassName();
        result = 31 * result + (internalClassName != null ? internalClassName.hashCode() : 0);
        return result;
    }
}
