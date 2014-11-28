/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.impl.eviction;

/**
 * Interface for checking about if eviction is required or not.
 *
 * @author sozal 28/11/14
 */
public interface EvictionChecker {

    /**
     * Checks for if eviction is required or not.
     *
     * @return <code>true</code> if eviction is required, otherwise <code>false</code>
     */
    boolean isEvictionRequired();

}
