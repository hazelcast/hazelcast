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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.core.Member;
import com.hazelcast.instance.NodeState;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Prints all kinds of Hazelcast member specific info.
 * <p>
 * Lots of other information is already captured through the metrics.
 */
public class MemberHazelcastInstanceInfoPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds the HazelcastMemberInstanceInfoPlugin runs.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(
            PREFIX + ".memberinfo.period.seconds", 60, SECONDS);

    private final long periodMillis;
    private final NodeEngineImpl nodeEngine;

    public MemberHazelcastInstanceInfoPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(MemberHazelcastInstanceInfoPlugin.class));
        this.periodMillis = nodeEngine.getProperties().getMillis(PERIOD_SECONDS);
        this.nodeEngine = nodeEngine;
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
        writer.startSection("HazelcastInstance");

        writer.writeKeyValueEntry("thisAddress", nodeEngine.getNode().getThisAddress().toString());
        writer.writeKeyValueEntry("isRunning", nodeEngine.getNode().isRunning());
        writer.writeKeyValueEntry("isLite", nodeEngine.getNode().isLiteMember());
        writer.writeKeyValueEntry("joined", nodeEngine.getNode().getClusterService().isJoined());
        NodeState state = nodeEngine.getNode().getState();
        writer.writeKeyValueEntry("nodeState", state == null ? "null" : state.toString());

        writer.writeKeyValueEntry("clusterId", nodeEngine.getClusterService().getClusterId());
        writer.writeKeyValueEntry("clusterSize", nodeEngine.getClusterService().getSize());
        writer.writeKeyValueEntry("isMaster", nodeEngine.getClusterService().isMaster());
        Address masterAddress = nodeEngine.getClusterService().getMasterAddress();
        writer.writeKeyValueEntry("masterAddress", masterAddress == null ? "null" : masterAddress.toString());

        writer.startSection("Members");
        for (Member member : nodeEngine.getClusterService().getMemberImpls()) {
            writer.writeEntry(member.getAddress().toString());
        }
        writer.endSection();

        writer.endSection();
    }
}
