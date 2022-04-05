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

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationMonitor;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.SECONDS;


/**
 * A diagnostics plugin that checks how the network is behaving by checking deviations in Operation-heartbeats to assist with
 * network related problems like split-brain.
 * <p>
 * It does this by checking the deviation in the interval between operation-heartbeat packets. Operation-heartbeat packets
 * are sent at a fixed interval (operation-call-timeout/4) and are not processed by operation-threads, but by their own system.
 * If there is a big deviation, then it could indicate networking problems. But it could also indicate other problems like JVM
 * issues.
 */
public class OperationHeartbeatPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     * <p>
     * This plugin is very cheap to run and is enabled by default.
     * <p>
     * It will also not print output every time it is executed, only when one or more members have a too high operation-heartbeat
     * deviation, the plugin will render output.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.operation-heartbeat.seconds", 10, SECONDS);

    /**
     * The maximum allowed deviation. E.g. with a default 60 call timeout and operation-heartbeat interval being 15 seconds,
     * the maximum deviation with a deviation-percentage of 33, is 5 seconds. So if a packet is arrived after 19 seconds; no
     * problem; but if it arrives after 21 seconds, then the plugin will render.
     */
    public static final HazelcastProperty MAX_DEVIATION_PERCENTAGE
            = new HazelcastProperty("hazelcast.diagnostics.operation-heartbeat.max-deviation-percentage", 33);

    private static final float HUNDRED = 100f;

    private final long periodMillis;
    private final long expectedIntervalMillis;
    private final int maxDeviationPercentage;
    private final ConcurrentMap<Address, AtomicLong> heartbeatPerMember;
    private boolean mainSectionStarted;

    public OperationHeartbeatPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(OperationHeartbeatPlugin.class));
        InvocationMonitor invocationMonitor = ((OperationServiceImpl) nodeEngine.getOperationService()).getInvocationMonitor();
        HazelcastProperties properties = nodeEngine.getProperties();
        this.periodMillis = properties.getMillis(PERIOD_SECONDS);
        this.maxDeviationPercentage = properties.getInteger(MAX_DEVIATION_PERCENTAGE);
        this.expectedIntervalMillis = invocationMonitor.getHeartbeatBroadcastPeriodMillis();
        this.heartbeatPerMember = invocationMonitor.getHeartbeatPerMember();
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + periodMillis + " max-deviation:" + maxDeviationPercentage + "%");
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        long nowMillis = System.currentTimeMillis();
        for (Map.Entry<Address, AtomicLong> entry : heartbeatPerMember.entrySet()) {
            Address member = entry.getKey();
            long lastHeartbeatMillis = entry.getValue().longValue();
            long noHeartbeatMillis = nowMillis - lastHeartbeatMillis;
            float deviation = HUNDRED * ((float) (noHeartbeatMillis - expectedIntervalMillis)) / expectedIntervalMillis;
            if (deviation >= maxDeviationPercentage) {
                startLazyMainSection(writer);

                writer.startSection("member" + member);
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
            writer.startSection("OperationHeartbeat");
        }
    }

    private void endLazyMainSection(DiagnosticsLogWriter writer) {
        if (mainSectionStarted) {
            mainSectionStarted = false;
            writer.endSection();
        }
    }
}
