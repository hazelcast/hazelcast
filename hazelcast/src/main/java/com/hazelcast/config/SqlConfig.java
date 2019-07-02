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

package com.hazelcast.config;

/**
 * SQL service configuration.
 */
public class SqlConfig {
    /** Default number of control threads. */
    public static final int DFLT_CONTROL_THREAD_COUNT = 2;

    /** Default number of data threads. */
    public static final int DFLT_DATA_THREAD_COUNT = Runtime.getRuntime().availableProcessors();

    private int controlThreadCount = DFLT_CONTROL_THREAD_COUNT;
    private int dataThreadCount = DFLT_DATA_THREAD_COUNT;

    /**
     * Get number of threads responsible for query coordination tasks.
     *
     * @return Number of threads responsible for query coordination tasks.
     */
    public int getControlThreadCount() {
        return controlThreadCount;
    }

    /**
     * Set the number of threads responsible for query coordination tasks.
     *
     * @param controlThreadCount Number of threads responsible for query coordination tasks.
     * @return This instance for chaining.
     */
    public SqlConfig setControlThreadCount(int controlThreadCount) {
        this.controlThreadCount = controlThreadCount;
        return this;
    }

    /**
     * Get the number of threads responsible for data processing.
     *
     * @return Number of threads responsible for data processing.
     */
    public int getDataThreadCount() {
        return dataThreadCount;
    }

    /**
     * Set the number of threads responsible for data processing.
     *
     * @param dataThreadCount Number of threads responsible for data processing.
     * @return This instance for chaining.
     */
    public SqlConfig setDataThreadCount(int dataThreadCount) {
        this.dataThreadCount = dataThreadCount;
        return this;
    }

    @Override
    public String toString() {
        return "SqlConfig{controlThreadCount=" + controlThreadCount + ", dataThreadCount=" + dataThreadCount + '}';
    }
}
