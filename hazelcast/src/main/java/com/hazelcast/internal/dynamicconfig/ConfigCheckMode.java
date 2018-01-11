/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.dynamicconfig;

/**
 * Behaviour when detects a configuration conflict while registering a new dynamic configuraiton.
 *
 */
public enum ConfigCheckMode {
    /**
     * Throw {@link com.hazelcast.config.ConfigurationException} when a configuration conflict is detected
     *
     */
    THROW_EXCEPTION,

    /**
     * Log a warning when detect a configuration conflict
     *
     */
    WARNING,

    /**
     * Ignore configuration conflicts.
     * The caller can still decide to a log it, but it should not go into WARNING level, but
     * into FINEST/DEBUG, etc..
     *
     */
    SILENT
}
