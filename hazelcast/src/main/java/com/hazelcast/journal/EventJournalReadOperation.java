/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.journal;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.ringbuffer.StaleSequenceException;
import com.hazelcast.ringbuffer.impl.ReadResultSetImpl;
import com.hazelcast.spi.DistributedObjectNamespace;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.version.Version;

import java.io.IOException;

/**
 * Reads from the map event journal in batches. You may specify the start sequence,
 * the maximum number of items in the response, a predicate that the events should
 * pass and a projection to apply to the events in the journal.
 * If the event journal currently contains no events, the response will be empty.
 * The predicate, filter and projection may be {@code null} in which case all elements are returned
 * and no projection is applied.
 *
 * @param <T> the return type of the projection. It is equal to the journal event type
 *            if the projection is {@code null} or it is the identity projection
 * @param <J> journal event type
 * @since 3.9
 */
public abstract class EventJournalReadOperation<T, J> extends Operation
        implements IdentifiedDataSerializable, PartitionAwareOperation {
    protected String name;
    protected int maxSize;
    protected long startSequence;

    protected transient ReadResultSetImpl<J, T> resultSet;
    protected transient DistributedObjectNamespace namespace;

    public EventJournalReadOperation() {
    }

    public EventJournalReadOperation(String name, long startSequence, int maxSize) {
        this.name = name;
        this.maxSize = maxSize;
        this.startSequence = startSequence;
    }

    /**
     * {@inheritDoc}
     * Checks the precondition that the start sequence is already
     * available (in the event journal) or is the sequence of the
     * next event to be added into the journal.
     */
    @Override
    public void beforeRun() {
        final Version clusterVersion = getNodeEngine().getClusterService().getClusterVersion();
        if (clusterVersion.isLessThan(Versions.V3_9)) {
            throw new UnsupportedOperationException(
                    "Event journal actions are not available when cluster version is " + clusterVersion);
        }

        namespace = new DistributedObjectNamespace(getServiceName(), name);

        final EventJournal<J> journal = getJournal();
        if (!journal.hasEventJournal(namespace)) {
            throw new UnsupportedOperationException(
                    "Cannot subscribe to event journal because it is either not configured or disabled for " + namespace);
        }

        final int partitionId = getPartitionId();
        journal.cleanup(namespace, partitionId);
        journal.isAvailableOrNextSequence(namespace, partitionId, startSequence);
    }

    @Override
    public void run() throws Exception {
        final EventJournal<J> journal = getJournal();
        final int partitionId = getPartitionId();
        journal.cleanup(namespace, partitionId);

        resultSet = createResultSet();
        if (!journal.isNextAvailableSequence(namespace, partitionId, startSequence)) {
            journal.readMany(namespace, partitionId, startSequence, resultSet);
        }
    }

    @Override
    public Object getResponse() {
        return resultSet;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeInt(maxSize);
        out.writeLong(startSequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        maxSize = in.readInt();
        startSequence = in.readLong();
    }

    @Override
    public void logError(Throwable e) {
        if (e instanceof StaleSequenceException) {
            ILogger logger = getLogger();
            if (logger.isFinestEnabled()) {
                logger.finest(e.getMessage(), e);
            } else if (logger.isFineEnabled()) {
                logger.fine(e.getClass().getSimpleName() + ": " + e.getMessage());
            }
        } else {
            super.logError(e);
        }
    }

    public abstract String getServiceName();

    protected abstract ReadResultSetImpl<J, T> createResultSet();

    protected abstract EventJournal<J> getJournal();
}
