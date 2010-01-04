/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

public class ExecutorConfig {

    public final static int DEFAULT_CORE_POOL_SIZE = 10;
    public final static int DEFAULT_MAX_POOL_SIZE = 50;
    public final static int DEFAULT_KEEPALIVE_SECONDS = 60;

    private int corePoolSize = DEFAULT_CORE_POOL_SIZE;

    private int maxPoolSize = DEFAULT_MAX_POOL_SIZE;

    private int keepAliveSeconds = DEFAULT_KEEPALIVE_SECONDS;

    /**
     * @return the corePoolSize
     */
    public int getCorePoolSize() {
        return corePoolSize;
    }

    /**
     * @param corePoolSize the corePoolSize to set
     */
    public ExecutorConfig setCorePoolSize(final int corePoolSize) {
        if (corePoolSize <= 0) {
            throw new IllegalArgumentException("corePoolSize must be positive");
        }
        this.corePoolSize = corePoolSize;
        return this;
    }

    /**
     * @return the maxPoolSize
     */
    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    /**
     * @param maxPoolSize the maxPoolSize to set
     */
    public ExecutorConfig setMaxPoolSize(final int maxPoolSize) {
        if (maxPoolSize <= 0) {
            throw new IllegalArgumentException("maxPoolSize must be positive");
        }
        this.maxPoolSize = maxPoolSize;
        return this;
    }

    /**
     * @return the keepAliveSeconds
     */
    public int getKeepAliveSeconds() {
        return keepAliveSeconds;
    }

    /**
     * @param keepAliveSeconds the keepAliveSeconds to set
     */
    public ExecutorConfig setKeepAliveSeconds(final int keepAliveSeconds) {
        if (keepAliveSeconds <= 0) {
            throw new IllegalArgumentException("keepAlice seconds must be positive");
        }
        this.keepAliveSeconds = keepAliveSeconds;
        return this;
    }
}
