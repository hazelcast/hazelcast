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
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.AbstractNamedOperation;

import java.io.IOException;

/**
 * Cache ManagementConfig Operation provides enabling/disabling the functionality of  management and
 * statistics mxbeans of the cache.
 * @see com.hazelcast.cache.impl.AbstractHazelcastCacheManager#enableManagement(String, boolean)
 * @see com.hazelcast.cache.impl.AbstractHazelcastCacheManager#enableStatistics(String, boolean)
 * @see com.hazelcast.cache.impl.ICacheService#setManagementEnabled(com.hazelcast.config.CacheConfig, String, boolean)
 * @see com.hazelcast.cache.impl.ICacheService#setStatisticsEnabled(com.hazelcast.config.CacheConfig, String, boolean)
 */
public class CacheManagementConfigOperation
        extends AbstractNamedOperation
        implements IdentifiedDataSerializable {

    private boolean isStat;
    private boolean enabled;

    public CacheManagementConfigOperation() {
    }

    public CacheManagementConfigOperation(String cacheNameWithPrefix, boolean isStat, boolean enabled) {
        super(cacheNameWithPrefix);
        this.isStat = isStat;
        this.enabled = enabled;
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void run()
            throws Exception {
        final ICacheService service = getService();
        if (isStat) {
            service.setStatisticsEnabled(null, name, enabled);
        } else {
            service.setManagementEnabled(null, name, enabled);
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeBoolean(isStat);
        out.writeBoolean(enabled);
    }

    @Override
    protected void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        isStat = in.readBoolean();
        enabled = in.readBoolean();
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.MANAGEMENT_CONFIG;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

}
