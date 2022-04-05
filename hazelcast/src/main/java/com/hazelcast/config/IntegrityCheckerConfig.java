/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
 * Configures Module Integrity Checker. For now the only configuration
 * option is whether the integrity checker is enabled or not.
 */
public class IntegrityCheckerConfig {
    private boolean enabled;

    public IntegrityCheckerConfig() { }

    /**
     * Returns {true} if the integrity checker is enabled. Integrity checker
     * performs checks to verify that the executable from which
     * HazelcastInstance is launched contains all the necessary
     * resources/META-INF/services files to operate correctly.
     *
     * @return {true} if the integrity checker is enabled
     * @since 5.1
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the flag to enable or disable integrity checker. Integrity checker
     * performs checks to verify that Integrity checker performs checks to
     * verify that the executable from which HazelcastInstance is launched
     * contains all the necessary resources/META-INF/services files to operate
     * correctly. This operation is compute-intensive and therefore recommended
     * being disabled for production clusters where built executable integrity
     * is already verified.
     *
     * @param enabled to enable or disable integrity checker
     * @return this config instance
     * @since 5.1
     */
    public IntegrityCheckerConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }
}
