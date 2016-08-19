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

package com.hazelcast.cache.impl.operation;

import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.config.CacheConfig;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.Operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PostJoinCacheOperation extends Operation {

    private List<CacheConfig> configs = new ArrayList<CacheConfig>();

    public void addCacheConfig(CacheConfig cacheConfig) {
        configs.add(cacheConfig);
    }

    @Override
    public String getServiceName() {
        return ICacheService.SERVICE_NAME;
    }

    @Override
    public void run() throws Exception {
        ICacheService cacheService = getService();
        for (CacheConfig cacheConfig : configs) {
            cacheService.putCacheConfigIfAbsent(cacheConfig);
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

}
