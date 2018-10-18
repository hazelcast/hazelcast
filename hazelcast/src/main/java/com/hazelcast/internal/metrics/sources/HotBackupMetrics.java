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

package com.hazelcast.internal.metrics.sources;

import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.ProbeLevel;

public final class HotBackupMetrics implements MetricsSource {

    private final NodeExtension nodeExtension;

    public HotBackupMetrics(NodeExtension nodeExtension) {
        this.nodeExtension = nodeExtension;
    }

    @Override
    public void collectAll(CollectionCycle cycle) {
        if (cycle.isCollected(ProbeLevel.INFO)) {
            HotRestartService hotRestartService = nodeExtension.getHotRestartService();
            boolean enabled = hotRestartService.isHotBackupEnabled();
            cycle.switchContext().namespace("hot-backup");
            cycle.collectAll(hotRestartService);
            if (enabled) {
                cycle.collectAll(hotRestartService.getBackupTaskStatus());
            }
        }
    }
}
