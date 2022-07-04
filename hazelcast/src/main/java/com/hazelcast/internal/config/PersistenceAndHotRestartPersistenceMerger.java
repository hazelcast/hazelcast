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

package com.hazelcast.internal.config;

import com.hazelcast.config.HotRestartClusterDataRecoveryPolicy;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.PersistenceClusterDataRecoveryPolicy;
import com.hazelcast.config.PersistenceConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.Objects;

public final class PersistenceAndHotRestartPersistenceMerger {

    private static final ILogger LOGGER = Logger.getLogger(PersistenceAndHotRestartPersistenceMerger.class);

    private PersistenceAndHotRestartPersistenceMerger() {

    }

    /**
     * if hot-restart-persistence: enabled="true" and persistence: enabled="false"
     * => enable persistence (HR) and use the config from hot-restart-persistence.
     * Does not break current deployments.
     *
     * <br><br>
     *
     * if hot-restart-persistence: enabled="false" and persistence: enabled="true"
     * => enable persistence (HR) and use the config from persistence. This is
     * for the new users.
     *
     * <br><br>
     *
     * if hot-restart-persistence: enabled="true" and persistence: enabled="true"
     * => enable persistence (HR) and use the config from persistence. We prefer
     * the new element, and the old one might get removed at some point.
     *
     * if hot-restart-persistence: enabled="false" and persistence: enabled="false"
     * => we still do override hot-restart-persistence using persistence.
     * It is necessary to maintain equality consistency.
     *
     * @param hotRestartPersistenceConfig hotRestartPersistenceConfig to use in the merge
     * @param persistenceConfig persistenceConfig to use in the merge
     */
    public static void merge(HotRestartPersistenceConfig hotRestartPersistenceConfig, PersistenceConfig persistenceConfig) {
        if (equals(hotRestartPersistenceConfig, persistenceConfig)) {
            return;
        }
        if (hotRestartPersistenceConfig.isEnabled() && !persistenceConfig.isEnabled()) {
            persistenceConfig.setEnabled(true)
                    .setBaseDir(hotRestartPersistenceConfig.getBaseDir())
                    .setBackupDir(hotRestartPersistenceConfig.getBackupDir())
                    .setAutoRemoveStaleData(hotRestartPersistenceConfig.isAutoRemoveStaleData())
                    .setEncryptionAtRestConfig(hotRestartPersistenceConfig.getEncryptionAtRestConfig())
                    .setDataLoadTimeoutSeconds(hotRestartPersistenceConfig.getDataLoadTimeoutSeconds())
                    .setParallelism(hotRestartPersistenceConfig.getParallelism())
                    .setValidationTimeoutSeconds(hotRestartPersistenceConfig.getValidationTimeoutSeconds())
                    .setClusterDataRecoveryPolicy(PersistenceClusterDataRecoveryPolicy
                            .valueOf(hotRestartPersistenceConfig.getClusterDataRecoveryPolicy().name()));
            return;
        }

        boolean override = hotRestartPersistenceConfig.isEnabled() && persistenceConfig.isEnabled();

        hotRestartPersistenceConfig.setEnabled(persistenceConfig.isEnabled())
                .setBaseDir(persistenceConfig.getBaseDir())
                .setBackupDir(persistenceConfig.getBackupDir())
                .setAutoRemoveStaleData(persistenceConfig.isAutoRemoveStaleData())
                .setEncryptionAtRestConfig(persistenceConfig.getEncryptionAtRestConfig())
                .setDataLoadTimeoutSeconds(persistenceConfig.getDataLoadTimeoutSeconds())
                .setParallelism(persistenceConfig.getParallelism())
                .setValidationTimeoutSeconds(persistenceConfig.getValidationTimeoutSeconds())
                .setClusterDataRecoveryPolicy(HotRestartClusterDataRecoveryPolicy.
                        valueOf(persistenceConfig.getClusterDataRecoveryPolicy().name()));

        if (override) {
            LOGGER.warning(
                    "Please note that HotRestartPersistence is deprecated and should not be used. "
                            + "Since both HotRestartPersistence and Persistence are enabled, "
                            + "and thus there is a conflict, the latter is used in persistence configuration."
            );
        }
    }

    private static boolean equals(HotRestartPersistenceConfig hotRestartPersistenceConfig, PersistenceConfig persistenceConfig) {
        if (hotRestartPersistenceConfig.isEnabled() != persistenceConfig.isEnabled()) {
            return false;
        }
        if (! Objects.equals(hotRestartPersistenceConfig.getBaseDir(), persistenceConfig.getBaseDir())) {
            return false;
        }
        if (! Objects.equals(hotRestartPersistenceConfig.getBackupDir(), persistenceConfig.getBackupDir())) {
            return false;
        }
        if (! Objects.equals(hotRestartPersistenceConfig.isAutoRemoveStaleData(), persistenceConfig.isAutoRemoveStaleData())) {
            return false;
        }
        if (! Objects.equals(hotRestartPersistenceConfig.getEncryptionAtRestConfig(),
                persistenceConfig.getEncryptionAtRestConfig())) {
            return false;
        }
        if (hotRestartPersistenceConfig.getClusterDataRecoveryPolicy().ordinal()
                != persistenceConfig.getClusterDataRecoveryPolicy().ordinal()) {
            return false;
        }
        return hotRestartPersistenceConfig.getDataLoadTimeoutSeconds() == persistenceConfig.getDataLoadTimeoutSeconds()
                && hotRestartPersistenceConfig.getParallelism() == persistenceConfig.getParallelism()
                && hotRestartPersistenceConfig.getValidationTimeoutSeconds() == persistenceConfig.getValidationTimeoutSeconds();
    }
}
