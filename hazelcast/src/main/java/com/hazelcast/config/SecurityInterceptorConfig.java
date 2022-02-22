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

package com.hazelcast.config;

import com.hazelcast.security.SecurityInterceptor;

/**
 * This class is used to configure {@link SecurityInterceptor}
 */
public class SecurityInterceptorConfig {

    protected String className;

    protected SecurityInterceptor implementation;

    public SecurityInterceptorConfig() {
    }

    public SecurityInterceptorConfig(final String className) {
        this.className = className;
    }

    public SecurityInterceptorConfig(final SecurityInterceptor implementation) {
        this.implementation = implementation;
    }

    public String getClassName() {
        return className;
    }

    public SecurityInterceptorConfig setClassName(final String className) {
        this.className = className;
        return this;
    }

    public SecurityInterceptor getImplementation() {
        return implementation;
    }

    public SecurityInterceptorConfig setImplementation(final SecurityInterceptor implementation) {
        this.implementation = implementation;
        return this;
    }
}
