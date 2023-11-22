/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
 * Configuration base for config types with a factory class and its properties.
 *
 * @param <T> final child type
 */
public abstract class AbstractFactoryWithPropertiesConfig<T extends AbstractFactoryWithPropertiesConfig<T>>
        extends AbstractBaseFactoryWithPropertiesConfig<T> {

    protected boolean enabled;

    /**
     * Returns if this configuration is enabled.
     *
     * @return {@code true} if enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables and disables this configuration.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     */
    public T setEnabled(boolean enabled) {
        this.enabled = enabled;
        return self();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{"
                + "factoryClassName='" + factoryClassName + '\''
                + ", properties=" + properties
                + ", enabled=" + enabled
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        AbstractFactoryWithPropertiesConfig<?> otherConfig = (AbstractFactoryWithPropertiesConfig<?>) o;

        return Objects.equals(enabled, otherConfig.enabled)
            && super.equals(otherConfig);
    }

    @Override
    public int hashCode() {
        return super.hashCode() + (enabled ? 0 : 13);
    }

}
