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

import java.util.Objects;

/**
 * Configuration for the Discovery Strategy Auto Detection.
 * <p>
 * When enabled, it will walk through all available discovery strategies and detect the correct one for the current runtime
 * environment. For example, it will automatically use the {@code hazelcast-aws} plugin if run on an AWS instance.
 */
public class AutoDetectionConfig {
    private boolean enabled = true;

    /**
     * Checks whether the auto detection mechanism is enabled.
     *
     * @return {@code true} if enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables the auto detection join mechanism.
     *
     * @param enabled {@code true} to enable the auto detection join mechanism, {@code false} to disable
     * @return AutoDetectionConfig the updated AutoDetectionConfig config
     */
    public AutoDetectionConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public String toString() {
        return "AutoDetectionConfig{enabled=" + enabled + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AutoDetectionConfig that = (AutoDetectionConfig) o;
        return enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(enabled);
    }
}
