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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListenable;
import com.hazelcast.nio.ConnectionListener;
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
            = new HazelcastProperty(Diagnostics.PREFIX + ".systemlog.enabled", "true");

    /**
     * If logging partition migration is enabled. Because there can be so many partitions, logging the partition migration
     * can be very noisy.
     */
    public static final HazelcastProperty LOG_PARTITIONS
            = new HazelcastProperty(Diagnostics.PREFIX + ".systemlog.partitions", "false");

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
                nodeEngine.getNode().connectionManager,
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
            } else if (item instanceof MigrationEvent) {
                render(writer, (MigrationEvent) item);
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
                if (member.getAddress().equals(thisAddress)) {
                    if (first) {
                        writer.writeEntry(member.getAddress().toString() + ":this:master");
                    } else {
                        writer.writeEntry(member.getAddress().toString() + ":this");
                    }
                } else if (first) {
                    writer.writeEntry(member.getAddress().toString() + ":master");
                } else {
                    writer.writeEntry(member.getAddress().toString());
                }
                first = false;
            }
        }
        writer.endSection();

        // ending the outer section
        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, MigrationEvent event) {
        switch (event.getStatus()) {
            case STARTED:
                writer.startSection("MigrationStarted");
                break;
            case COMPLETED:
                writer.startSection("MigrationCompleted");
                break;
            case FAILED:
                writer.startSection("MigrationFailed");
                break;
            default:
                return;
        }

        Member oldOwner = event.getOldOwner();
        writer.writeKeyValueEntry("oldOwner", oldOwner == null ? "null" : oldOwner.getAddress().toString());
        writer.writeKeyValueEntry("newOwner", event.getNewOwner().getAddress().toString());
        writer.writeKeyValueEntry("partitionId", event.getPartitionId());
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

        writer.writeKeyValueEntry("type", connection.getType().name());
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

    private class LifecycleListenerImpl implements LifecycleListener {
        @Override
        public void stateChanged(LifecycleEvent event) {
            logQueue.add(event);
        }
    }

    private static final class ConnectionEvent {
        final boolean added;
        final Connection connection;

        private ConnectionEvent(boolean added, Connection connection) {
            this.added = added;
            this.connection = connection;
        }
    }

    private class ConnectionListenerImpl implements ConnectionListener {
        @Override
        public void connectionAdded(Connection connection) {
            logQueue.add(new ConnectionEvent(true, connection));
        }

        @Override
        public void connectionRemoved(Connection connection) {
            logQueue.add(new ConnectionEvent(false, connection));
        }
    }

    private class MembershipListenerImpl extends MembershipAdapter {
        @Override
        public void memberAdded(MembershipEvent event) {
            logQueue.add(event);
        }

        @Override
        public void memberRemoved(MembershipEvent event) {
            logQueue.add(event);
        }
    }

    private class MigrationListenerImpl implements MigrationListener {
        @Override
        public void migrationStarted(MigrationEvent event) {
            logQueue.add(event);
        }

        @Override
        public void migrationCompleted(MigrationEvent event) {
            logQueue.add(event);
        }

        @Override
        public void migrationFailed(MigrationEvent event) {
            logQueue.add(event);
        }
    }

    private class ClusterVersionListenerImpl implements ClusterVersionListener {
        @Override
        public void onClusterVersionChange(Version newVersion) {
            logQueue.add(newVersion);
        }
    }
}
