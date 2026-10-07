/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.snapshot;

import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefService;
import com.hazelcast.cp.internal.datastructures.atomicref.AtomicRefSnapshot;
import com.hazelcast.cp.internal.datastructures.cpmap.CPMapServiceUtil;
import com.hazelcast.cp.internal.raft.ChunkedSnapshotAwareService;
import com.hazelcast.cp.internal.raft.impl.RaftEndpoint;
import com.hazelcast.cp.internal.raft.impl.RaftIntegration;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotChunk;
import com.hazelcast.cp.internal.raft.impl.log.SnapshotEntry;
import com.hazelcast.cp.internal.raftop.snapshot.RestoreSnapshotOp;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.servicemanager.ServiceInfo;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.cp.internal.util.ListTypeUtils.firstElementIsInstanceOf;
import static com.hazelcast.memory.MemoryUnit.MEGABYTES;

public final class ChunkUtil {

    /**
     * Default size of a single snapshot chunk in megabytes.
     * <p>
     * The same size is used as a chunk for partition migrations.
     *
     * @see ClusterProperty#PARTITION_CHUNKED_MAX_MIGRATING_DATA_IN_MB
     */
    public static final long DEFAULT_CHUNK_SIZE_IN_MB = 250;

    /**
     * Property name for configuring the maximum snapshot chunk size (in MB).
     * <p>
     * The value must be the same across all nodes.
     */
    public static final String PROP_MAX_CHUNK_SIZE_IN_MB = "hazelcast.cp.snapshot.chunk.max.size.mb";

    /**
     * Hazelcast property defining the maximum chunk size.
     */
    public static final HazelcastProperty MAX_CHUNK_SIZE_IN_MB =
            new HazelcastProperty(PROP_MAX_CHUNK_SIZE_IN_MB, DEFAULT_CHUNK_SIZE_IN_MB);

    private ChunkUtil() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    /**
     * Creates a list of {@link SnapshotChunk} objects from a given
     * snapshot. The snapshot is divided into chunks based on whether
     * the services support chunked snapshotting.
     * <p>
     * Non-chunked services are merged into the
     * last chunk to optimize the number of chunks.
     *
     * @param raftIntegration The raft integration
     * @param snapshotTerm    The term of the snapshot.
     * @param commitIndex     The commit index associated with the snapshot.
     * @param members         The group members at the time of the
     *                        snapshot.
     * @param membersIndex    The log index of the group
     *                        members.
     * @param snapshot        The snapshot data, which
     *                        must be a {@link Map} of service information to service
     *                        snapshots.
     * @param logger          The logger
     * @return A list of {@link SnapshotChunk} objects
     * representing the snapshot data, with metadata such as
     * chunk numbers, group members, and snapshot term populated.
     */
    @Nonnull
    public static List<SnapshotChunk> createChunksFromSnapshot(RaftIntegration raftIntegration,
                                                               int snapshotTerm, long commitIndex,
                                                               Collection<RaftEndpoint> members,
                                                               long membersIndex, Object snapshot,
                                                               ILogger logger) {
        // If the snapshot is already in the chunked format, return it as-is
        if (snapshot instanceof List<?> list
                && firstElementIsInstanceOf(list, SnapshotChunk.class)) {
            return (List<SnapshotChunk>) snapshot;
        }

        assert snapshot instanceof Map : "Snapshot must be a Map";

        // Step 1: Process the snapshot and create chunks
        Map<?, ?> snapshotPerServiceInfo = (Map<?, ?>) snapshot;
        List<SnapshotChunk> chunkedServiceSnapshots = new ArrayList<>();
        List<Object> nonChunkedServiceOps = new ArrayList<>();

        processSnapshotServices(raftIntegration, snapshotPerServiceInfo, commitIndex,
                chunkedServiceSnapshots, nonChunkedServiceOps, logger);

        // Step 2: Optimize chunks by merging non-chunked operations into the last chunk
        optimizeChunks(chunkedServiceSnapshots, nonChunkedServiceOps);

        // Step 3: Add metadata to chunks and prepare the final list
        return finalizeChunks(chunkedServiceSnapshots, snapshotTerm, commitIndex, members, membersIndex);
    }

    /**
     * Processes the snapshot services and categorizes them into chunked and non-chunked operations.
     */
    private static void processSnapshotServices(RaftIntegration raftIntegration,
                                                Map<?, ?> snapshotPerServiceInfo,
                                                long commitIndex,
                                                List<SnapshotChunk> chunkedServiceSnapshots,
                                                List<Object> nonChunkedServiceOps,
                                                ILogger logger) {
        for (Map.Entry<?, ?> entry : snapshotPerServiceInfo.entrySet()) {
            ServiceInfo serviceInfo = (ServiceInfo) entry.getKey();
            Object serviceSnapshot = entry.getValue();

            if (serviceInfo.getService() instanceof ChunkedSnapshotAwareService) {
                processChunkedService(raftIntegration, serviceInfo,
                        (Iterator<DataChunkGroup<?>>) serviceSnapshot,
                        commitIndex, chunkedServiceSnapshots, logger);
            } else {
                processNonChunkedService(raftIntegration, serviceInfo, serviceSnapshot,
                        commitIndex, nonChunkedServiceOps, logger);
            }
        }
    }

    /**
     * Processes a chunked service and adds its operations to the chunked list.
     */
    private static void processChunkedService(RaftIntegration raftIntegration, ServiceInfo serviceInfo,
                                              Iterator<DataChunkGroup<?>> iterator, long commitIndex,
                                              List<SnapshotChunk> chunkedServiceSnapshots, ILogger logger) {
        int createdChunkCount = 0;
        while (iterator.hasNext()) {
            DataChunkGroup<?> dataChunk = iterator.next();
            Object operation = raftIntegration.newRestoreSnapshotOp(serviceInfo.getName(), commitIndex, dataChunk);
            chunkedServiceSnapshots.add(createSnapshotChunkAndSetOperation(operation));
            createdChunkCount++;
        }

        logger.fine("Snapshot creation for chunked service '%s': created %d chunks at commitIndex=%d",
                serviceInfo.getName(), createdChunkCount, commitIndex);

    }

    /**
     * Processes a non-chunked service and adds its operation to the non-chunked list.
     */
    private static void processNonChunkedService(RaftIntegration raftIntegration, ServiceInfo serviceInfo,
                                                 Object serviceSnapshot, long commitIndex,
                                                 List<Object> notChunkedServiceOps, ILogger logger) {
        Object operation = raftIntegration.newRestoreSnapshotOp(serviceInfo.getName(), commitIndex, serviceSnapshot);
        notChunkedServiceOps.add(operation);

        logger.fine("Snapshot creation for non-chunked service '%s': 1 operation created at commitIndex=%d",
                serviceInfo.getName(), commitIndex);

    }

    /**
     * Optimizes snapshot chunks by merging non-chunked operations into the last chunk.
     * <p>
     * Chunk size calculations are approximate. Non-chunked services are assumed to have small data,
     * which may lead to occasional chunk size overflows.
     */
    private static void optimizeChunks(List<SnapshotChunk> chunkedServiceSnapshots,
                                       List<Object> notChunkedServiceOps) {
        if (notChunkedServiceOps.isEmpty()) {
            return;
        }

        if (chunkedServiceSnapshots.isEmpty()) {
            // Create a new chunk for non-chunked operations
            chunkedServiceSnapshots.add(createSnapshotChunkAndSetOperation(notChunkedServiceOps));
            return;
        }

        // Merge non-chunked operations into the last chunk
        SnapshotChunk lastChunk = chunkedServiceSnapshots.get(chunkedServiceSnapshots.size() - 1);
        List<Object> existingSnapshotOps = (List<Object>) lastChunk.operation();
        existingSnapshotOps.addAll(notChunkedServiceOps);
    }

    /**
     * Finalizes the chunks by adding metadata and returning the final list.
     */
    private static List<SnapshotChunk> finalizeChunks(List<SnapshotChunk> chunkedServiceSnapshots,
                                                      int snapshotTerm, long commitIndex,
                                                      Collection<RaftEndpoint> members, long membersIndex) {
        int chunkCount = chunkedServiceSnapshots.size();
        List<SnapshotChunk> finalChunks = new ArrayList<>(chunkCount);

        for (int chunkNumber = 0; chunkNumber < chunkCount; chunkNumber++) {
            SnapshotChunk chunk = chunkedServiceSnapshots.get(chunkNumber);
            chunk.setChunkCount(chunkCount)
                    .setChunkNumber(chunkNumber)
                    .setGroupMembers(members)
                    .setGroupMembersLogIndex(membersIndex)
                    .setSnapshotTerm(snapshotTerm)
                    .setIndex(commitIndex);

            finalChunks.add(chunk);
        }

        return finalChunks;
    }

    /**
     * Creates a new {@link SnapshotChunk} instance
     * and populates it with the given object.
     * <p>
     * If the given object is not an instance of a {@link
     * List}, it is first wrapped in a {@link List} object
     * and then set to the new {@link SnapshotChunk}.
     *
     * @param object either a single object or a {@link List} of objects
     * @return a new {@link SnapshotChunk} object
     */
    private static SnapshotChunk createSnapshotChunkAndSetOperation(Object object) {
        SnapshotChunk chunk = new SnapshotChunk();

        // when object is instance of a list of operations
        if (object instanceof List<?>) {
            chunk.setOperation(object);
        } else {
            // when object is a single operation
            List list = new ArrayList<>();
            list.add(object);
            chunk.setOperation(list);
        }

        return chunk;
    }

    /**
     * Helper method to extract list of chunks from a {@link SnapshotEntry}.
     *
     * @param snapshotEntry the snapshot entry
     * @return chunk list of a snapshot
     * @see #optimizeChunks(List, List)
     */
    public static List<SnapshotChunk> extractChunksFrom(@Nonnull SnapshotEntry snapshotEntry) {
        return ((List<SnapshotChunk>) snapshotEntry.operation());
    }

    /**
     * Helper method to return max chunk size of a snapshot chunk.
     */
    public static long getMaxChunkSizeInBytes(HazelcastProperties properties) {
        return MEGABYTES.toBytes(properties.getLong(MAX_CHUNK_SIZE_IN_MB));
    }

    /**
     * Groups data chunks belonging to a specific service (e.g.,
     * {@link AtomicRefService}).
     *
     * <p>Chunks such as {@link ValueDataChunk} or {@link KeyValueDataChunk} are combined
     * into {@link DataChunkGroup} instances. Each group’s total size will not exceed
     * the limit defined by {@link ChunkUtil#MAX_CHUNK_SIZE_IN_MB}. If a chunk itself
     * exceeds the size limit, it will be placed in a separate group.
     *
     * <p>For a visual example of how chunks are grouped, refer to {@link DataChunkGroup}:
     * <pre>
     * Input Chunks:    [KeyValueDataChunk1][KeyValueDataChunk2][KeyValueDataChunk3]
     * Grouped As:      [ Group1: KeyValueDataChunk1, KeyValueDataChunk2 ]
     *                  [ Group2: KeyValueDataChunk3 ]
     * </pre>
     *
     * @param dataChunks          the list of chunks to be grouped (e.g., {@link ValueDataChunk}, {@link KeyValueDataChunk})
     * @param maxChunkSizeInBytes the maximum allowed size (in bytes) per group
     * @return a list of chunk groups, each within the size constraint
     */
    public static <T extends AbstractDataChunk> List<DataChunkGroup<T>> groupServiceChunksBySize(@Nonnull List<T> dataChunks,
                                                                                                 final long maxChunkSizeInBytes) {
        List<DataChunkGroup<T>> groupedChunks = new ArrayList<>();

        DataChunkGroup<T> currentGroup = new DataChunkGroup<>();
        long currentGroupSize = 0;

        // Sort the provided dataChunks in descending order to reduce the number of DataChunkGroups.
        dataChunks.sort(Comparator.comparingLong(AbstractDataChunk::getChunkSizeInBytes).reversed());

        for (T chunk : dataChunks) {
            final long chunkSize = getChunkSize(chunk);

            if (chunkSize >= maxChunkSizeInBytes) {
                if (!currentGroup.isEmpty()) {
                    groupedChunks.add(currentGroup);
                    currentGroup = new DataChunkGroup<>();
                    currentGroupSize = 0;
                }
                currentGroup.add(chunk);
                groupedChunks.add(currentGroup);
                currentGroup = new DataChunkGroup<>();
            } else if (currentGroupSize + chunkSize <= maxChunkSizeInBytes) {
                currentGroup.add(chunk);
                currentGroupSize += chunkSize;
            } else {
                groupedChunks.add(currentGroup);
                currentGroup = new DataChunkGroup<>();
                currentGroup.add(chunk);
                currentGroupSize = chunkSize;
            }
        }

        if (!currentGroup.isEmpty()) {
            groupedChunks.add(currentGroup);
        }

        return groupedChunks;
    }

    /**
     * Calculates the size in bytes of a given chunk object.
     * Supports {@link KeyValueDataChunk} and {@link ValueDataChunk} types.
     *
     * @param chunk The chunk object whose size needs to be determined
     * @return The size of the chunk in bytes
     * @throws IllegalArgumentException if the chunk object is not of a supported type
     */
    private static long getChunkSize(Object chunk) {
        if (chunk instanceof KeyValueDataChunk dataChunk) {
            return dataChunk.getChunkSizeInBytes();
        }

        if (chunk instanceof ValueDataChunk dataChunk) {
            return dataChunk.getChunkSizeInBytes();
        }

        throw new IllegalArgumentException("Unknown chunk object type " + chunk);
    }

    /**
     * Processes a single snapshot chunk and distributes its operations to either service snapshots or direct ops.
     */
    private static void processChunk(SnapshotChunk chunk, Map<String, Object> serviceSnapshots,
                                     List<RestoreSnapshotOp> snapshotOps) {
        Object op = chunk.operation();
        if (!(op instanceof List<?>)) {
            return;
        }

        for (RestoreSnapshotOp restoreOp : (List<RestoreSnapshotOp>) op) {
            String serviceName = restoreOp.getServiceName();
            Object snapshotData = restoreOp.getSnapshot();

            switch (serviceName) {
                case CPMapServiceUtil.SERVICE_NAME:
                    handleCPMapSnapshot(serviceSnapshots, snapshotData);
                    break;
                case AtomicRefService.SERVICE_NAME:
                    handleAtomicRefSnapshot(serviceSnapshots, snapshotData);
                    break;
                default:
                    snapshotOps.add(restoreOp);
                    break;
            }
        }
    }

    /**
     * Handles CPMap snapshot data chunk processing.
     */
    private static void handleCPMapSnapshot(Map<String, Object> serviceSnapshots, Object snapshotData) {
        if (!(snapshotData instanceof DataChunkGroup<?>)) {
            return;
        }

        DataChunkGroup<?> dataChunkGroup = (DataChunkGroup<?>) snapshotData;
        Object cpMapSnapshot = serviceSnapshots.computeIfAbsent(CPMapServiceUtil.SERVICE_NAME,
                s -> newCPMapRegistrySnapshot());
        invokeCPMapFromChunks(cpMapSnapshot, dataChunkGroup.getServiceData());
    }

    private static Object newCPMapRegistrySnapshot() {
        try {
            return Class.forName("com.hazelcast.cp.internal.datastructures.cpmap.CPMapRegistrySnapshot")
                        .getConstructor()
                        .newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("CPMap registry snapshot type is not available", e);
        }
    }

    private static void invokeCPMapFromChunks(Object cpMapSnapshot, List<?> chunks) {
        try {
            cpMapSnapshot.getClass().getMethod("fromChunks", List.class).invoke(cpMapSnapshot, chunks);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to reconstruct CPMap registry snapshot from chunks", e);
        }
    }

    /**
     * Handles AtomicRef snapshot data chunk processing.
     */
    private static void handleAtomicRefSnapshot(Map<String, Object> serviceSnapshots, Object snapshotData) {
        if (!(snapshotData instanceof DataChunkGroup<?>)) {
            return;
        }

        DataChunkGroup<ValueDataChunk> dataChunkGroup = (DataChunkGroup<ValueDataChunk>) snapshotData;
        AtomicRefSnapshot atomicRefSnapshot = (AtomicRefSnapshot) serviceSnapshots.computeIfAbsent(
                AtomicRefService.SERVICE_NAME,
                s -> new AtomicRefSnapshot()
        );
        atomicRefSnapshot.fromChunks(dataChunkGroup.getServiceData());
    }
}
