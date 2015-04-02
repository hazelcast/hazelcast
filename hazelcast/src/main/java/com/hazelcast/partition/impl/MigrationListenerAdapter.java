package com.hazelcast.partition.impl;

import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationEvent.MigrationStatus;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.partition.PartitionEventListener;

/**
 * Wraps a migration listener and dispatches a migration event to matching method
 */
class MigrationListenerAdapter implements PartitionEventListener<MigrationEvent> {

    private final MigrationListener migrationListener;

    public MigrationListenerAdapter(MigrationListener migrationListener) {
        this.migrationListener = migrationListener;
    }

    @Override
    public void onEvent(MigrationEvent migrationEvent) {

        final MigrationStatus status = migrationEvent.getStatus();
        switch (status) {
            case STARTED:
                migrationListener.migrationStarted(migrationEvent);
                break;
            case COMPLETED:
                migrationListener.migrationCompleted(migrationEvent);
                break;
            case FAILED:
                migrationListener.migrationFailed(migrationEvent);
                break;
            default:
                throw new IllegalArgumentException("Not a known MigrationStatus: " + status);
        }

    }

}
