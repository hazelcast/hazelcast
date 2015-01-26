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

package com.hazelcast.cache.impl.nearcache;

import com.hazelcast.nio.serialization.SerializationService;

/**
 * Context to hold all required external services and utilities to be used by
 * {@link com.hazelcast.cache.impl.nearcache.NearCache}.
 */
public class NearCacheContext {

    private SerializationService serializationService;
    private NearCacheExecutor nearCacheExecutor;

    public NearCacheContext() {

    }

    public NearCacheContext(SerializationService serializationService,
                            NearCacheExecutor nearCacheExecutor) {
        this.serializationService = serializationService;
        this.nearCacheExecutor = nearCacheExecutor;
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public NearCacheContext setSerializationService(SerializationService serializationService) {
        this.serializationService = serializationService;
        return this;
    }

    public NearCacheExecutor getNearCacheExecutor() {
        return nearCacheExecutor;
    }

    public NearCacheContext setNearCacheExecutor(NearCacheExecutor nearCacheExecutor) {
        this.nearCacheExecutor = nearCacheExecutor;
        return this;
    }

}
