/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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

public final class QueueConfig {

    public final static int DEFAULT_MAX_SIZE_PER_JVM = Integer.MAX_VALUE;
    public final static int DEFAULT_TTL_SECONDS = Integer.MAX_VALUE;

    private String name;

    private int maxSizePerJVM = DEFAULT_MAX_SIZE_PER_JVM;

    private int timeToLiveSeconds = DEFAULT_TTL_SECONDS;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public QueueConfig setName(String name) {
        this.name = name;
        return this;
    }

    /**
     * @return the maxSizePerJVM
     */
    public int getMaxSizePerJVM() {
        return maxSizePerJVM;
    }

    /**
     * @param maxSizePerJVM the maxSizePerJVM to set
     */
    public QueueConfig setMaxSizePerJVM(int maxSizePerJVM) {
        this.maxSizePerJVM = maxSizePerJVM;
        return this;
    }

    /**
     * @return the timeToLiveSeconds
     */
    public int getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    /**
     * @param timeToLiveSeconds the timeToLiveSeconds to set
     */
    public QueueConfig setTimeToLiveSeconds(int timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }
}
