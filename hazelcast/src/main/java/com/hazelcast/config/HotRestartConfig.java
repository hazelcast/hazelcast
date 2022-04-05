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

import com.hazelcast.internal.config.ConfigDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Configures the Hot Restart Persistence per Hazelcast data structure.
 *
 * <br><br>
 * Note: If either, but not both, data-persistence ({@code DataPersistenceConfig}) or
 * hot-restart ({@code HotRestartConfig}) is enabled,
 * Hazelcast will use the configuration of the enabled element. If both are
 * enabled, Hazelcast will use the data-persistence ({@code DataPersistenceConfig})
 * configuration. hot-restart element (and thus {@code HotRestartConfig})
 * will be removed in a future release.
 *
 * @deprecated since 5.0 use {@link DataPersistenceConfig}
 */
@Deprecated
public class HotRestartConfig implements IdentifiedDataSerializable {

    private boolean enabled;
    private boolean fsync;

    public HotRestartConfig() {
    }

    public HotRestartConfig(HotRestartConfig hotRestartConfig) {
        enabled = hotRestartConfig.enabled;
        fsync = hotRestartConfig.fsync;
    }

    /**
     * Returns whether hot restart enabled on related data structure.
     *
     * @return true if hot restart enabled, false otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets whether hot restart is enabled on related data structure.
     *
     * @return HotRestartConfig
     */
    public HotRestartConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns whether disk write should be followed by an {@code fsync()} system call.
     *
     * @return true if fsync is be called after disk write, false otherwise
     */
    public boolean isFsync() {
        return fsync;
    }

    /**
     * Sets whether disk write should be followed by an {@code fsync()} system call.
     *
     * @param fsync fsync
     * @return this HotRestartConfig
     */
    public HotRestartConfig setFsync(boolean fsync) {
        this.fsync = fsync;
        return this;
    }

    @Override
    public String toString() {
        return "HotRestartConfig{"
                + "enabled=" + enabled
                + ", fsync=" + fsync
                + '}';
    }

    @Override
    public int getFactoryId() {
        return ConfigDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ConfigDataSerializerHook.HOT_RESTART_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(enabled);
        out.writeBoolean(fsync);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        enabled = in.readBoolean();
        fsync = in.readBoolean();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HotRestartConfig)) {
            return false;
        }

        HotRestartConfig that = (HotRestartConfig) o;
        if (enabled != that.enabled) {
            return false;
        }
        return fsync == that.fsync;
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (fsync ? 1 : 0);
        return result;
    }
}
