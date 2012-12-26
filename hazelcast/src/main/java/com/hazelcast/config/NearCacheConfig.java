/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NearCacheConfig implements DataSerializable {
    public final static int DEFAULT_TTL_SECONDS = 0;
    public final static int DEFAULT_MAX_IDLE_SECONDS = 0;
    public final static int DEFAULT_MAX_SIZE = Integer.MAX_VALUE;
    public final static String DEFAULT_EVICTION_POLICY = "LRU";

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    private int maxSize = DEFAULT_MAX_SIZE;

    private String evictionPolicy = DEFAULT_EVICTION_POLICY;

    private int maxIdleSeconds = DEFAULT_MAX_IDLE_SECONDS;

    private boolean invalidateOnChange = true;

    public NearCacheConfig(int timeToLiveSeconds, int maxSize, String evictionPolicy, int maxIdleSeconds, boolean invalidateOnChange) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        this.maxSize = maxSize;
        this.evictionPolicy = evictionPolicy;
        this.maxIdleSeconds = maxIdleSeconds;
        this.invalidateOnChange = invalidateOnChange;
    }

    public NearCacheConfig() {
    }

    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public NearCacheConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public NearCacheConfig setMaxSize(int maxSize) {
        this.maxSize = maxSize;
        return this;
    }

    public String getEvictionPolicy() {
        return evictionPolicy;
    }

    public NearCacheConfig setEvictionPolicy(String evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public NearCacheConfig setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = maxIdleSeconds;
        return this;
    }

    public boolean isInvalidateOnChange() {
        return invalidateOnChange;
    }

    public NearCacheConfig setInvalidateOnChange(boolean invalidateOnChange) {
        this.invalidateOnChange = invalidateOnChange;
        return this;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(timeToLiveSeconds);
        out.writeInt(maxSize);
        out.writeBoolean(evictionPolicy == null);
        if (evictionPolicy != null)
            out.writeUTF(evictionPolicy);
        out.writeInt(maxIdleSeconds);
        out.writeBoolean(invalidateOnChange);
    }

    public void readData(DataInput in) throws IOException {
        timeToLiveSeconds = in.readInt();
        maxSize = in.readInt();
        boolean nullEvictionPolicy = in.readBoolean();
        if (!nullEvictionPolicy) evictionPolicy = in.readUTF();
        else evictionPolicy = null;
        maxIdleSeconds = in.readInt();
        invalidateOnChange = in.readBoolean();
    }
}
