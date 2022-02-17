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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterHeartbeatManager;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A diagnostics plugin that checks the quality of member/member heartbeats.
 * <p>
 * Normally, heartbeats are sent at a fixed frequency, but if there is a
 * deviation in this frequency, it could indicate problems.
 */
public class MemberHeartbeatPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds the MemberHeartbeatPlugin runs.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * This plugin will only output if there is the max deviation is exceeded.
     * <p>
     * Setting the value high will lead to not seeing smaller deviations. E.g
     * if this plugin runs every minute, then it will not see a lot of small
     * deviations. The default of 10 seconds is ok since it will not generate
     * too much overhead and noise and in most cases it is the big outliers
     * we are interested in.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(
            "hazelcast.diagnostics.member-heartbeat.period.seconds", 10, SECONDS);

    /**
     * The maximum allowed deviation. E.g. if the interval of member/member
     * heartbeats is 5 seconds, a 100% deviation will be fine with heartbeats
     * arriving up to 5 seconds after they are expected. So a heartbeat arriving
     * at 9 seconds will not be rendered, but a heartbeat received at 11 seconds,
     * will be rendered.
     */
    public static final HazelcastProperty MAX_DEVIATION_PERCENTAGE
            = new HazelcastProperty("hazelcast.diagnostics.member-heartbeat.max-deviation-percentage", 100);

    private static final float HUNDRED = 100f;

    private final long periodMillis;
    private final NodeEngineImpl nodeEngine;
    private final int maxDeviationPercentage;
    private boolean mainSectionStarted;

    public MemberHeartbeatPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(MemberHazelcastInstanceInfoPlugin.class));
        this.nodeEngine = nodeEngine;
        HazelcastProperties properties = nodeEngine.getProperties();
        this.periodMillis = properties.getMillis(PERIOD_SECONDS);
        this.maxDeviationPercentage = properties.getInteger(MAX_DEVIATION_PERCENTAGE);
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active, period-millis:" + periodMillis);
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        ClusterService cs = nodeEngine.getClusterService();
        if (!(cs instanceof ClusterServiceImpl)) {
            // lets be lenient for testing etc if some kind of mocked cluster service is encountered;
            // we don't want to cause problems
            return;
        }
        render(writer, (ClusterServiceImpl) cs);
    }

    private void render(DiagnosticsLogWriter writer, ClusterServiceImpl clusterService) {
        ClusterHeartbeatManager clusterHeartbeatManager = clusterService.getClusterHeartbeatManager();
        long expectedIntervalMillis = clusterHeartbeatManager.getHeartbeatIntervalMillis();

        long nowMillis = System.currentTimeMillis();
        for (MemberImpl member : clusterService.getMemberImpls()) {
            long lastHeartbeatMillis = clusterHeartbeatManager.getLastHeartbeatTime(member);
            if (lastHeartbeatMillis == 0L) {
                // member without a heartbeat; lets skip it
                continue;
            }

            long noHeartbeatMillis = nowMillis - lastHeartbeatMillis;
            float deviation = HUNDRED * ((float) (noHeartbeatMillis - expectedIntervalMillis)) / expectedIntervalMillis;
            if (deviation >= maxDeviationPercentage) {
                startLazyMainSection(writer);

                writer.startSection("member" + member.getAddress());
                writer.writeKeyValueEntry("deviation(%)", deviation);
                writer.writeKeyValueEntry("noHeartbeat(ms)", noHeartbeatMillis);
                writer.writeKeyValueEntry("lastHeartbeat(ms)", lastHeartbeatMillis);
                writer.writeKeyValueEntryAsDateTime("lastHeartbeat(date-time)", lastHeartbeatMillis);
                writer.writeKeyValueEntry("now(ms)", nowMillis);
                writer.writeKeyValueEntryAsDateTime("now(date-time)", nowMillis);
                writer.endSection();
            }
        }

        endLazyMainSection(writer);
    }

    private void startLazyMainSection(DiagnosticsLogWriter writer) {
        if (!mainSectionStarted) {
            mainSectionStarted = true;
            writer.startSection("MemberHeartbeats");
        }
    }

    private void endLazyMainSection(DiagnosticsLogWriter writer) {
        if (mainSectionStarted) {
            mainSectionStarted = false;
            writer.endSection();
        }
    }
}
