/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitors;

import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * A {@link PerformanceMonitorPlugin} that displays the build info.
 */
public class BuildInfoPlugin extends PerformanceMonitorPlugin {

    private final BuildInfo buildInfo = BuildInfoProvider.getBuildInfo();
    private final ILogger logger;

    public BuildInfoPlugin(NodeEngineImpl nodeEngine) {
        this.logger = nodeEngine.getLogger(BuildInfoPlugin.class);
    }

    @Override
    public long getPeriodMillis() {
        return STATIC;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active");
    }

    @Override
    public void run(PerformanceLogWriter writer) {
        writer.startSection("BuildInfo");
        writer.writeKeyValueEntry("Build", buildInfo.getBuild());
        // we convert to string to prevent formatting the number
        writer.writeKeyValueEntry("BuildNumber", "" + buildInfo.getBuildNumber());
        writer.writeKeyValueEntry("Revision", buildInfo.getRevision());
        writer.writeKeyValueEntry("Version", buildInfo.getVersion());
        writer.writeKeyValueEntry("SerialVersion", buildInfo.getSerializationVersion());
        writer.writeKeyValueEntry("Enterprise", buildInfo.isEnterprise());
        writer.endSection();
    }
}
