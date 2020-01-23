/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
 * Contains the configuration for Hazelcast Management Center.
 */
public final class ManagementCenterConfig {

    private boolean scriptingEnabled;

    public ManagementCenterConfig() {
    }

    /**
     * Enables/disables scripting on the member. Management center allows to send a script for execution to a member. The script
     * can access the unerlying system Hazelcast member is running on with permissions of user running the member. Disabling
     * scripting improves the member security.
     * <p>
     * Default value for this config element depends on Hazelcast edition: {@code false} for Hazelcast Enterprise, {@code true}
     * for Hazelcast (open source).
     *
     * @param scriptingEnabled {@code true} to allow scripting on the member, {@code false} to disallow
     * @return this management center config instance
     * @since 3.12
     */
    public ManagementCenterConfig setScriptingEnabled(final boolean scriptingEnabled) {
        this.scriptingEnabled = scriptingEnabled;
        return this;
    }

    /**
     * Returns if scripting is allowed ({@code true}) or disallowed ({@code false}).
     */
    public boolean isScriptingEnabled() {
        return scriptingEnabled;
    }

    @Override
    public String toString() {
        return "ManagementCenterConfig{"
                + "scriptingEnabled=" + scriptingEnabled
                + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ManagementCenterConfig that = (ManagementCenterConfig) o;
        return scriptingEnabled == that.scriptingEnabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(scriptingEnabled);
    }
}
