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

/**
 * MapStore configuration
 */
public final class MapStoreConfig {
    private boolean enabled = true;
    private String className = null;
    private int writeDelaySeconds = DEFAULT_WRITE_DELAY_SECONDS;

    public static final int DEFAULT_WRITE_DELAY_SECONDS = 0;

    /**
     * Returns the name of the MapStore implementation class
     *
     * @return the name of the class
     */
    public String getClassName() {
        return className;
    }

    /**
     * Sets the name fo the MapStore implementation class
     *
     * @param className the name of the MapStore implementation class to set
     */
    public void setClassName(String className) {
        this.className = className;
    }

    /**
     * Returns the number of seconds to delay the store writes.
     *
     * @return the number of delay seconds
     */
    public int getWriteDelaySeconds() {
        return writeDelaySeconds;
    }

    /**
     * Sets the number of seconds to delay before writing (storing) the dirty records
     *
     * @param writeDelaySeconds the number of seconds to delay
     */
    public void setWriteDelaySeconds(int writeDelaySeconds) {
        this.writeDelaySeconds = writeDelaySeconds;
    }

    /**
     * Returns if this configuration is enabled
     *
     * @return true if enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration
     *
     * @param enabled
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }
}
