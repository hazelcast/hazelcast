/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

public final class DataPersistenceAndHotRestartMerger {

    private static final ILogger LOGGER = Logger.getLogger(DataPersistenceAndHotRestartMerger.class);

    private DataPersistenceAndHotRestartMerger() {

    }

    /**
     * if hot-restart: enabled="true" and data-persistence: enabled="false"
     * => enable persistence and use the config from hot-restart.
     * Does not break current deployments.
     *
     * <br><br>
     *
     * if hot-restart: enabled="false" and data-persistence: enabled="true"
     * => enable persistence and use the config from data-persistence. This is
     * for the new users.
     *
     * <br><br>
     *
     * if hot-restart: enabled="true" and data-persistence: enabled="true"
     * => enable persistence and use the config from data-persistence. We prefer
     * the new element, and the old one might get removed at some point.
     *
     * @return true if hotRestartConfig has been overridden by
     * dataPersistenceConfig
     */
    public static void merge(HotRestartConfig hotRestartConfig, DataPersistenceConfig dataPersistenceConfig) {

        if (hotRestartConfig.isEnabled() && !dataPersistenceConfig.isEnabled()) {
            dataPersistenceConfig.setEnabled(true).setFsync(hotRestartConfig.isFsync());
            return;
        }

        if (!dataPersistenceConfig.isEnabled()) {
            return;
        }

        boolean override = hotRestartConfig.isEnabled() && dataPersistenceConfig.isEnabled();

        hotRestartConfig.setEnabled(dataPersistenceConfig.isEnabled())
                .setFsync(dataPersistenceConfig.isFsync());

        if (override) {
            LOGGER.warning(
                    "Please note that HotRestart is deprecated and should not be used. "
                    + "Since both HotRestart and DataPersistence are enabled, "
                    + "and thus there is a conflict, the latter is used in persistence configuration."
            );
        }
    }
}
