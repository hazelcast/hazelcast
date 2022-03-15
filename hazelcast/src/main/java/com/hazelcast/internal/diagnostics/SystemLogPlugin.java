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
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipAdapter;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListenable;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.ILogger;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.version.Version;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * The {@link SystemLogPlugin} is responsible for:
 * <ol>
 * <li>Showing lifecycle changes like shutting down, merging etc.</li>
 * <li>Show connection creation and connection closing. Also the causes of closes are included.</li>
 * <li>Showing membership changes.</li>
 * <li>Optionally showing partition migration</li>
 * </ol>
 * This plugin is very useful to get an idea what is happening inside a cluster;
 * especially when there are connection related problems.
 * <p>
 * This plugin has a low overhead and is meant to run in production.
 */
public class SystemLogPlugin extends DiagnosticsPlugin {

    /**
     * If this plugin is enabled.
     */
    public static final HazelcastProperty ENABLED
            = new HazelcastProperty("hazelcast.diagnostics.systemlog.enabled", "true");

    /**
     * If logging partition migration is enabled. Because there can be so many partitions, logging the partition migration
     * can be very noisy.
     */
    public static final HazelcastProperty LOG_PARTITIONS
            = new HazelcastProperty("hazelcast.diagnostics.systemlog.partitions", "false");

    /**
     * Currently the Diagnostic is scheduler based, so each task gets to run as often at is has been configured. This works
     * fine for most plugins, but if there are outside events triggering the need to write, this scheduler based model is
     * not the right fit. In the future the Diagnostics need to be improved.
     */
    private static final long PERIOD_MILLIS = SECONDS.toMillis(1);

    private final Queue<Object> logQueue = new ConcurrentLinkedQueue<Object>();
    private final ConnectionListenable connectionObservable;
    private final HazelcastInstance hazelcastInstance;
    private final Address thisAddress;
    private final boolean logPartitions;
    private final boolean enabled;
    private final NodeExtension nodeExtension;

    public SystemLogPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getProperties(),
                nodeEngine.getNode().getServer(),
                nodeEngine.getHazelcastInstance(),
                nodeEngine.getLogger(SystemLogPlugin.class),
                nodeEngine.getNode().getNodeExtension());
    }

    public SystemLogPlugin(HazelcastProperties properties,
                           ConnectionListenable connectionObservable,
                           HazelcastInstance hazelcastInstance,
                           ILogger logger) {
        this(properties, connectionObservable, hazelcastInstance, logger, null);
    }

    public SystemLogPlugin(HazelcastProperties properties,
                           ConnectionListenable connectionObservable,
                           HazelcastInstance hazelcastInstance,
                           ILogger logger,
                           NodeExtension nodeExtension) {
        super(logger);
        this.connectionObservable = connectionObservable;
        this.hazelcastInstance = hazelcastInstance;
        this.thisAddress = getThisAddress(hazelcastInstance);
        this.logPartitions = properties.getBoolean(LOG_PARTITIONS);
        this.enabled = properties.getBoolean(ENABLED);
        this.nodeExtension = nodeExtension;
    }

    private Address getThisAddress(HazelcastInstance hazelcastInstance) {
        try {
            return hazelcastInstance.getCluster().getLocalMember().getAddress();
        } catch (UnsupportedOperationException e) {
            return null;
        }
    }

    @Override
    public long getPeriodMillis() {
        if (!enabled) {
            return DiagnosticsPlugin.DISABLED;
        }
        return PERIOD_MILLIS;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: logPartitions:" + logPartitions);

        connectionObservable.addConnectionListener(new ConnectionListenerImpl());
        hazelcastInstance.getCluster().addMembershipListener(new MembershipListenerImpl());
        if (logPartitions) {
            hazelcastInstance.getPartitionService().addMigrationListener(new MigrationListenerImpl());
        }
        hazelcastInstance.getLifecycleService().addLifecycleListener(new LifecycleListenerImpl());
        if (nodeExtension != null) {
            nodeExtension.registerListener(new ClusterVersionListenerImpl());
        }
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        for (; ; ) {
            Object item = logQueue.poll();
            if (item == null) {
                return;
            }

            if (item instanceof LifecycleEvent) {
                render(writer, (LifecycleEvent) item);
            } else if (item instanceof MembershipEvent) {
                render(writer, (MembershipEvent) item);
            } else if (item instanceof MigrationState) {
                render(writer, (MigrationState) item);
            } else if (item instanceof ReplicaMigrationEvent) {
                render(writer, (ReplicaMigrationEvent) item);
            } else if (item instanceof ConnectionEvent) {
                ConnectionEvent event = (ConnectionEvent) item;
                render(writer, event);
            } else if (item instanceof Version) {
                render(writer, (Version) item);
            }
        }
    }

    private void render(DiagnosticsLogWriter writer, LifecycleEvent event) {
        writer.startSection("Lifecycle");
        writer.writeEntry(event.getState().name());
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, MembershipEvent event) {
        switch (event.getEventType()) {
            case MembershipEvent.MEMBER_ADDED:
                writer.startSection("MemberAdded");
                break;
            case MembershipEvent.MEMBER_REMOVED:
                writer.startSection("MemberRemoved");
                break;
            default:
                return;
        }
        writer.writeKeyValueEntry("member", event.getMember().getAddress().toString());

        // writing the members
        writer.startSection("Members");
        Set<Member> members = event.getMembers();
        if (members != null) {
            boolean first = true;
            for (Member member : members) {
                Address memberAddress = member.getAddress();
                String addressStr = String.valueOf(memberAddress);
                if (memberAddress.equals(thisAddress)) {
                    if (first) {
                        writer.writeEntry(addressStr + ":this:master");
                    } else {
                        writer.writeEntry(addressStr + ":this");
                    }
                } else if (first) {
                    writer.writeEntry(addressStr + ":master");
                } else {
                    writer.writeEntry(addressStr);
                }
                first = false;
            }
        }
        writer.endSection();

        // ending the outer section
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, MigrationState migrationState) {
        writer.startSection("MigrationState");
        writer.writeKeyValueEntryAsDateTime("startTime", migrationState.getStartTime());
        writer.writeKeyValueEntry("plannedMigrations", migrationState.getPlannedMigrations());
        writer.writeKeyValueEntry("completedMigrations", migrationState.getCompletedMigrations());
        writer.writeKeyValueEntry("remainingMigrations", migrationState.getRemainingMigrations());
        writer.writeKeyValueEntry("totalElapsedTime(ms)", migrationState.getTotalElapsedTime());
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, ReplicaMigrationEvent event) {
        if (event.isSuccess()) {
            writer.startSection("MigrationCompleted");
        } else {
            writer.startSection("MigrationFailed");
        }

        Member source = event.getSource();
        writer.writeKeyValueEntry("source", source == null ? "null" : source.getAddress().toString());
        writer.writeKeyValueEntry("destination", event.getDestination().getAddress().toString());
        writer.writeKeyValueEntry("partitionId", event.getPartitionId());
        writer.writeKeyValueEntry("replicaIndex", event.getReplicaIndex());
        writer.writeKeyValueEntry("elapsedTime(ms)", event.getReplicaIndex());

        render(writer, event.getMigrationState());
        writer.endSection();
    }

    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void render(DiagnosticsLogWriter writer, ConnectionEvent event) {
        if (event.added) {
            writer.startSection("ConnectionAdded");
        } else {
            writer.startSection("ConnectionRemoved");
        }

        Connection connection = event.connection;
        writer.writeEntry(connection.toString());

        if (connection instanceof ServerConnection) {
            writer.writeKeyValueEntry("type", ((ServerConnection) connection).getConnectionType());
        }
        writer.writeKeyValueEntry("isAlive", connection.isAlive());

        if (!event.added) {
            String closeReason = connection.getCloseReason();
            Throwable closeCause = connection.getCloseCause();
            if (closeReason == null && closeCause != null) {
                closeReason = closeCause.getMessage();
            }

            writer.writeKeyValueEntry("closeReason", closeReason == null ? "Unknown" : closeReason);

            if (closeCause != null) {
                writer.startSection("CloseCause");
                String s = closeCause.getClass().getName();
                String message = closeCause.getMessage();
                writer.writeEntry((message != null) ? (s + ": " + message) : s);

                for (StackTraceElement element : closeCause.getStackTrace()) {
                    writer.writeEntry(element.toString());
                }
                writer.endSection();
            }
        }
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, Version version) {
        writer.startSection("ClusterVersionChanged");
        writer.writeEntry(version.toString());
        writer.endSection();
    }

    protected class LifecycleListenerImpl implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            logQueue.add(event);
        }
    }

    private static final class ConnectionEvent {
        final boolean added;
        final Connection connection;

        ConnectionEvent(boolean added, Connection connection) {
            this.added = added;
            this.connection = connection;
        }
    }

    protected class ConnectionListenerImpl implements ConnectionListener {
        @Override
        public void connectionAdded(Connection connection) {
            logQueue.add(new ConnectionEvent(true, connection));
        }

        @Override
        public void connectionRemoved(Connection connection) {
            logQueue.add(new ConnectionEvent(false, connection));
        }
    }

    protected class MembershipListenerImpl extends MembershipAdapter {
        @Override
        public void memberAdded(MembershipEvent event) {
            logQueue.add(event);
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            logQueue.add(event);
        }
    }

    protected class MigrationListenerImpl implements MigrationListener {
        @Override
        public void migrationStarted(MigrationState state) {
            logQueue.add(state);
        }

        @Override
        public void migrationFinished(MigrationState state) {
            logQueue.add(state);
        }

        @Override
        public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            logQueue.add(event);
        }

        @Override
        public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            logQueue.add(event);
        }
    }

    protected class ClusterVersionListenerImpl implements ClusterVersionListener {
        @Override
        public void onClusterVersionChange(Version newVersion) {
            logQueue.add(newVersion);
        }
    }
}
