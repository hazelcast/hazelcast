/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.management;

import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MaxSizePolicy;

public class UpdateMapConfigParameters {
    private final String map;
    private final int timeToLiveSeconds;
    private final int maxIdleSeconds;
    private final EvictionPolicy evictionPolicy;
    private final boolean readBackupData;
    private final int maxSize;
    private final MaxSizePolicy maxSizePolicy;

    public UpdateMapConfigParameters(String map,
                                     int timeToLiveSeconds,
                                     int maxIdleSeconds,
                                     EvictionPolicy evictionPolicy,
                                     boolean readBackupData,
                                     int maxSize,
                                     MaxSizePolicy maxSizePolicy) {
        this.map = map;
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxIdleSeconds = maxIdleSeconds;
        this.evictionPolicy = evictionPolicy;
        this.readBackupData = readBackupData;
        this.maxSize = maxSize;
        this.maxSizePolicy = maxSizePolicy;
    }

    public String getMap() {
        return map;
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

    public boolean isReadBackupData() {
        return readBackupData;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public MaxSizePolicy getMaxSizePolicy() {
        return maxSizePolicy;
    }
}
