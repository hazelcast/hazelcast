/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.nearcache;

import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Context to hold all required external services and utilities to be used by
 * {@link com.hazelcast.cache.impl.nearcache.NearCache}.
 */
public class NearCacheContext {

    private final SerializationService serializationService;
    private final ExecutionService executionService;
    private final ClassLoader classLoader;

    private NearCacheManager nearCacheManager;

    public NearCacheContext(SerializationService serializationService,
                            ExecutionService executionService) {
        this(serializationService, executionService, null, null);
    }

    public NearCacheContext(SerializationService serializationService,
                            ExecutionService executionService,
                            NearCacheManager nearCacheManager) {
        this(serializationService, executionService, nearCacheManager, null);
    }

    public NearCacheContext(SerializationService serializationService,
                            ExecutionService executionService,
                            NearCacheManager nearCacheManager,
                            ClassLoader classLoader) {
        this.serializationService = serializationService;
        this.executionService = executionService;
        this.classLoader = classLoader;

        this.nearCacheManager = nearCacheManager;
    }

    public NearCacheManager getNearCacheManager() {
        return nearCacheManager;
    }

    public void setNearCacheManager(NearCacheManager nearCacheManager) {
        this.nearCacheManager = nearCacheManager;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ExecutionService getExecutionService() {
        return executionService;
    }

    public ClassLoader getClassLoader() {
        return classLoader;
    }
}
