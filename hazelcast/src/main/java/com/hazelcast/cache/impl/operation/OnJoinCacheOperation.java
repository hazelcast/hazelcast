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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.CacheDataSerializerHook;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.exception.ServiceNotFoundException;

import javax.cache.configuration.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.cache.impl.ICacheService.SERVICE_NAME;
import static com.hazelcast.cache.impl.JCacheDetector.isJCacheAvailable;

/**
 * Operation executed on joining members so they become aware of {@link CacheConfig}s dynamically created via
 * {@link javax.cache.CacheManager#createCache(String, Configuration)}. Depending on the cluster version, this operation
 * is executed either as a post-join operation (when cluster version is &lt; 3.9) or as a pre-join operation (since 3.9), to
 * resolve a race between the {@link CacheConfig} becoming available in the joining member and creation of a
 * {@link com.hazelcast.cache.ICache} proxy.
 */
public class OnJoinCacheOperation extends Operation implements IdentifiedDataSerializable {

    private List<CacheConfig> configs = new ArrayList<CacheConfig>();

    public void addCacheConfig(CacheConfig cacheConfig) {
        configs.add(cacheConfig);
    }

    @Override
    public String getServiceName() {
        return SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        if (isJCacheAvailable(getNodeEngine().getConfigClassLoader())) {
            ICacheService cacheService = getService();
            for (CacheConfig cacheConfig : configs) {
                cacheService.putCacheConfigIfAbsent(cacheConfig);
            }
        } else {
            // if JCache is not in classpath and no Cache configurations need to be processed, do not fail the operation
            // instead log a warning that if JCache API will be used then it will fail.
            if (configs.isEmpty()) {
                getLogger().warning("This member is joining a cluster whose members support JCache, however the cache-api "
                        + "artifact is missing from this member's classpath. In case JCache API will be used, add cache-api "
                        + "artifact in this member's classpath and restart the member.");
            } else {
                // JCache is already in use by other cluster members, so log an informative message to resolve the issue and
                // throw the CacheService not found exception.
                getLogger().severe("This member cannot support JCache because the cache-api artifact is missing from "
                        + "its classpath. Add the JCache API JAR in the classpath and restart the member.");
                throw new HazelcastException("Service with name '" + SERVICE_NAME + "' not found!",
                        new ServiceNotFoundException("Service with name '" + SERVICE_NAME + "' not found!"));
            }
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(configs.size());
        for (CacheConfig config : configs) {
            out.writeObject(config);
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int confSize = in.readInt();
        for (int i = 0; i < confSize; i++) {
            CacheConfig config = in.readObject();
            configs.add(config);
        }
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.CACHE_POST_JOIN;
    }
}
