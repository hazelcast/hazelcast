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

package com.hazelcast.core;

/**
 * Hazelcast distributed semaphore helper class can be used to dynamically
 * interact with certain semaphore events. You can override the helper
 * methods for the desired usage.
 * <p/>
 * Semaphore sample config:
 * <semaphore name="default">
 * <initial-permits>1</initial-permits>
 * <helper-class-name>com.domain.MySemaphoreHelper</helper-class-name>
 * </semaphore>
 */
public interface SemaphoreFactory {
    /**
     * Gets the initial permit value of a given named semaphore.
     * <p/>
     * Implementation can get the initial permits value using any means;
     * such as a formula, querying the cluster, an O/R mapping tool,
     * simple SQL, reading from a file, etc.
     *
     * @param name
     * @return number of initial permits
     */
    public int getInitialPermits(String name, int configValue);
}

