/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.Collections.newSetFromMap;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.toList;

public class JobRepository {

    /**
     * Prefix of all Hazelcast internal objects used by Jet (such as job
     * metadata, snapshots etc.)
     */
    public static final String INTERNAL_JET_OBJECTS_PREFIX = "__jet.";

    /**
     * State snapshot exported using {@link Job#exportSnapshot(String)} is
     * currently stored in IMaps named with this prefix.
     */
    public static final String EXPORTED_SNAPSHOTS_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "exportedSnapshot.";

    /**
     * A cache to speed up access to details about exported snapshots.
     */
    public static final String EXPORTED_SNAPSHOTS_DETAIL_CACHE = INTERNAL_JET_OBJECTS_PREFIX + "exportedSnapshotsCache";

    /**
     * Name of internal IMap which stores job resources.
     */
    public static final String RESOURCES_MAP_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "resources.";

    /**
     * Name of internal IMap which is used for unique id generation.
     */
    public static final String RANDOM_IDS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "ids";

    /**
     * Name of internal IMap which stores {@link JobRecord}s.
     */
    public static final String JOB_RECORDS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "records";

    /**
     * Name of internal IMap which stores {@link JobExecutionRecord}s.
     */
    public static final String JOB_EXECUTION_RECORDS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "executionRecords";

    /**
     * Name of internal IMap which stores job results
     */
    public static final String JOB_RESULTS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "results";

    /**
     * Prefix for internal IMaps which store snapshot data. Snapshot data for
     * one snapshot is stored in either of the following two maps:
     * <ul>
     *     <li>{@code _jet.snapshot.<jobId>.0}
     *     <li>{@code _jet.snapshot.<jobId>.1}
     * </ul>
     * Which one of these is determined in {@link JobExecutionRecord}.
     */
    public static final String SNAPSHOT_DATA_MAP_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "snapshot.";

    private static final String RESOURCE_MARKER = "__jet.resourceMarker";
    private static final long DEFAULT_RESOURCES_EXPIRATION_MILLIS = HOURS.toMillis(2);

    private final HazelcastInstance instance;
    private final ILogger logger;

    private final IMap<Long, Long> randomIds;
    private final IMap<Long, JobRecord> jobRecords;
    private final IMap<Long, JobExecutionRecord> jobExecutionRecords;
    private final IMap<Long, JobResult> jobResults;
    private final IMap<String, SnapshotValidationRecord> exportedSnapshotDetailsCache;
    private long resourcesExpirationMillis = DEFAULT_RESOURCES_EXPIRATION_MILLIS;

    /**
     * Because the member can fail at any moment we try to delete job data regularly
     * for completed jobs. However, this creates overhead:
     * <pre>{@code
     *     IMap map = instance.getMap("map");
     *     map.destroy();
     * }</pre>
     *
     * To avoid it, we store the deleted jobIds in this set. If it's found there, we don't
     * retry to delete it.
     */
    private final Set<Long> deletedJobs = newSetFromMap(new ConcurrentHashMap<>());

    public JobRepository(JetInstance jetInstance) {
        this.instance = jetInstance.getHazelcastInstance();
        this.logger = instance.getLoggingService().getLogger(getClass());

        this.randomIds = instance.getMap(RANDOM_IDS_MAP_NAME);
        this.jobRecords = instance.getMap(JOB_RECORDS_MAP_NAME);
        this.jobExecutionRecords = instance.getMap(JOB_EXECUTION_RECORDS_MAP_NAME);
        this.jobResults = instance.getMap(JOB_RESULTS_MAP_NAME);
        this.exportedSnapshotDetailsCache = instance.getMap(EXPORTED_SNAPSHOTS_DETAIL_CACHE);
    }

    // for tests
    void setResourcesExpirationMillis(long resourcesExpirationMillis) {
        this.resourcesExpirationMillis = resourcesExpirationMillis;
    }

    /**
     * Uploads job resources and returns a unique job id generated for the job.
     * If the upload process fails for any reason, such as being unable to access a resource,
     * uploaded resources are cleaned up.
     */
    public long uploadJobResources(JobConfig jobConfig) {
        long jobId = newJobId();

        IMap<String, Object> jobResourcesMap = getJobResources(jobId);
        for (ResourceConfig rc : jobConfig.getResourceConfigs()) {
            Map<String, byte[]> tmpMap = new HashMap<>();
            if (rc.isArchive()) {
                try {
                    loadJar(tmpMap, rc.getUrl());
                } catch (IOException e) {
                    cleanupJobResourcesAndSnapshots(jobId, jobResourcesMap);
                    randomIds.remove(jobId);
                    throw new JetException("Job resource upload failed", e);
                }
            } else {
                try {
                    InputStream in = rc.getUrl().openStream();
                    readStreamAndPutCompressedToMap(rc.getId(), tmpMap, in);
                } catch (IOException e) {
                    cleanupJobResourcesAndSnapshots(jobId, jobResourcesMap);
                    randomIds.remove(jobId);
                    throw new JetException("Job resource upload failed", e);
                }
            }

            // now upload it all
            jobResourcesMap.putAll(tmpMap);
        }

        // the marker object will be used to decide when to clean up job resources
        jobResourcesMap.put(RESOURCE_MARKER, System.currentTimeMillis());

        return jobId;
    }

    private long newJobId() {
        long jobId;
        do {
            jobId = Util.secureRandomNextLong();
        } while (randomIds.putIfAbsent(jobId, jobId) != null);
        return jobId;
    }

    /**
     * Unzips the Jar archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(String, Map, InputStream)}.
     */
    private void loadJar(Map<String, byte[]> map, URL url) throws IOException {
        try (JarInputStream jis = new JarInputStream(new BufferedInputStream(url.openStream()))) {
            JarEntry jarEntry;
            while ((jarEntry = jis.getNextJarEntry()) != null) {
                if (jarEntry.isDirectory()) {
                    continue;
                }
                readStreamAndPutCompressedToMap(jarEntry.getName(), map, jis);
            }
        }
    }

    private void readStreamAndPutCompressedToMap(String resourceName,
                                                 Map<String, byte[]> map, InputStream in)
            throws IOException {
        // ignore duplicates: the first resource in first jar takes precedence
        if (map.containsKey(resourceName)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
            IOUtil.drainTo(in, compressor);
        }

        map.put(resourceName, baos.toByteArray());
    }

    private void cleanupJobResourcesAndSnapshots(long jobId, IMap<String, Object> jobResourcesMap) {
        destroySnapshotDataMaps(jobId);
        jobResourcesMap.destroy();
    }

    /**
     * Puts the given job record into the jobRecords map.
     * If another job record is already put, it checks if it has the same DAG.
     * If it has a different DAG, then the call fails with {@link IllegalStateException}
     */
    void putNewJobRecord(JobRecord jobRecord) {
        long jobId = jobRecord.getJobId();
        JobRecord prev = jobRecords.putIfAbsent(jobId, jobRecord);
        if (prev != null && !prev.getDag().equals(jobRecord.getDag())) {
            throw new IllegalStateException("Cannot put job record for job " + idToString(jobId)
                    + " because it already exists with a different DAG");
        }
    }

    /**
     * Updates the job quorum size of all jobs so that it is at least {@code
     * newQuorumSize}.
     */
    void updateJobQuorumSizeIfSmaller(long jobId, int newQuorumSize) {
        jobExecutionRecords.executeOnKey(jobId, Util.<Long, JobExecutionRecord>entryProcessor((key, value) -> {
            if (value == null) {
                return null;
            }
            value.setLargerQuorumSize(newQuorumSize);
            return value;
        }));
    }

    /**
     * Generates a new execution id for the given job id, guaranteed to be unique across the cluster
     */
    long newExecutionId(long jobId) {
        long executionId;
        do {
            executionId = Util.secureRandomNextLong();
        } while (randomIds.putIfAbsent(executionId, jobId) != null);
        return executionId;
    }

    /**
     * Returns how many execution ids are present for the given job id
     */
    long getExecutionIdCount(long jobId) {
        return randomIds.values(new FilterExecutionIdByJobIdPredicate(jobId)).size();
    }

    /**
     * Puts a JobResult for the given job and deletes the JobRecord.
     * @throws JobNotFoundException if the JobRecord is not found
     * @throws IllegalStateException if the JobResult is already present
     */
    void completeJob(long jobId, String coordinator, long completionTime, Throwable error) {
        JobRecord jobRecord = getJobRecord(jobId);
        if (jobRecord == null) {
            throw new JobNotFoundException(jobId);
        }

        JobConfig config = jobRecord.getConfig();
        long creationTime = jobRecord.getCreationTime();
        JobResult jobResult = new JobResult(jobId, config, coordinator, creationTime, completionTime,
                error != null ? error.toString() : null);

        JobResult prev = jobResults.putIfAbsent(jobId, jobResult);
        if (prev != null) {
            throw new IllegalStateException("Job result already exists in the " + jobResults.getName() + " map:\n" +
                    "previous record: " + prev + "\n" +
                    "new record: " + jobResult);
        }

        deleteJob(jobId);
    }

    /**
     * Performs cleanup after job completion. Deletes job record and job resources but keeps the job id
     * so that it will not be used again for a new job submission.
     */
    void deleteJob(long jobId) {
        if (deletedJobs.contains(jobId)) {
            return;
        }

        // Delete the job record
        jobExecutionRecords.remove(jobId);
        jobRecords.remove(jobId);
        // Delete the execution ids, but keep the job id
        randomIds.removeAll(new FilterExecutionIdByJobIdPredicate(jobId));

        // Delete job resources
        cleanupJobResourcesAndSnapshots(jobId, getJobResources(jobId));

        deletedJobs.add(jobId);
    }

    /**
     * Cleans up stale job records, execution ids and job resources.
     */
    void cleanup(Set<Long> runningJobIds) {
        // clean up completed jobRecords
        Set<Long> completedJobIds = jobResults.keySet();
        completedJobIds.forEach(this::deleteJob);

        Set<Long> validJobIds = new HashSet<>();
        validJobIds.addAll(completedJobIds);
        validJobIds.addAll(runningJobIds);
        validJobIds.addAll(jobRecords.keySet());

        // Job ids are never cleaned up.
        // We also don't clean up job records here because they might be started in parallel
        // while cleanup is running. If a job id is not running or is completed, it might be
        // suitable to clean up job resources.
        randomIds.keySet(new FilterJobIdPredicate())
                 .stream()
                 .filter(jobId -> !validJobIds.contains(jobId))
                 .forEach(jobId -> {
                     IMap<String, Object> resources = getJobResources(jobId);
                     EntryView<String, Object> marker = resources.getEntryView(RESOURCE_MARKER);
                     // If the marker is absent, then job resources may be still uploaded.
                     // Just put the marker so that the job resources may be cleaned up eventually.
                     // If the job resources are still being uploaded, then the marker will be overwritten, which is ok.
                     if (marker == null) {
                         resources.putIfAbsent(RESOURCE_MARKER, System.currentTimeMillis());
                     } else if (isMarkerExpired(marker)) {
                         // The marker has been around for defined expiry time and the job still wasn't started.
                         // We assume the job submission was interrupted - let's clean up the data.
                         cleanupJobResourcesAndSnapshots(jobId, resources);
                     }
                 });
    }

    private boolean isMarkerExpired(EntryView<String, Object> record) {
        return (System.currentTimeMillis() - (Long) record.getValue()) >= resourcesExpirationMillis;
    }

    List<JobRecord> getJobRecords(String name) {
        return jobRecords.values(new FilterJobRecordByNamePredicate(name)).stream()
                         .sorted(comparing(JobRecord::getCreationTime).reversed()).collect(toList());
    }

    Set<Long> getAllJobIds() {
        Set<Long> ids = new HashSet<>();
        ids.addAll(jobRecords.keySet());
        ids.addAll(jobResults.keySet());
        return ids;
    }

    Collection<JobRecord> getJobRecords() {
        return jobRecords.values();
    }

    public JobRecord getJobRecord(long jobId) {
       return jobRecords.get(jobId);
    }

    public JobExecutionRecord getJobExecutionRecord(long jobId) {
       return jobExecutionRecords.get(jobId);
    }

    <T> IMap<String, T> getJobResources(long jobId) {
        return instance.getMap(RESOURCES_MAP_NAME_PREFIX + idToString(jobId));
    }

    public JobResult getJobResult(long jobId) {
        return jobResults.get(jobId);
    }

    Collection<JobResult> getJobResults() {
        return jobResults.values();
    }

    List<JobResult> getJobResults(String name) {
        return jobResults.values(new FilterJobResultByNamePredicate(name)).stream()
                  .sorted(comparing(JobResult::getCreationTime).reversed()).collect(toList());
    }

    /**
     * Write the {@link JobExecutionRecord} to the IMap.
     * <p>
     * The write will be ignored if the timestamp of the given record is older
     * than the timestamp of the stored record. See {@link
     * UpdateJobExecutionRecordEntryProcessor#process}. It will also be ignored
     * if the key doesn't exist in the IMap.
     */
    void writeJobExecutionRecord(long jobId, JobExecutionRecord record, boolean canCreate) {
        record.updateTimestamp();
        String message = (String) jobExecutionRecords.executeOnKey(jobId,
                new UpdateJobExecutionRecordEntryProcessor(jobId, record, canCreate));
        if (message != null) {
            logger.fine(message);
        }
    }

    /**
     * Returns map name in the form {@code "_jet.snapshot.<jobId>.<dataMapIndex>"}.
     */
    public static String snapshotDataMapName(long jobId, int dataMapIndex) {
        return SNAPSHOT_DATA_MAP_PREFIX + idToString(jobId) + '.' + dataMapIndex;
    }

    /**
     * Returns map name in the form {@code "_jet.exportedSnapshot.<jobId>.<dataMapIndex>"}.
     */
    public static String exportedSnapshotMapName(String name) {
        return JobRepository.EXPORTED_SNAPSHOTS_PREFIX + name;
    }

    /**
     * Delete all snapshots for a given job.
     */
    private void destroySnapshotDataMaps(long jobId) {
        instance.getMap(snapshotDataMapName(jobId, 0)).destroy();
        instance.getMap(snapshotDataMapName(jobId, 1)).destroy();
        logFine(logger, "Destroyed both snapshot maps for job %s", idToString(jobId));
    }

    void clearSnapshotData(long jobId, int dataMapIndex) {
        String mapName = snapshotDataMapName(jobId, dataMapIndex);
        try {
            instance.getMap(mapName).clear();
            logFine(logger, "Cleared snapshot data map %s", mapName);
        } catch (Exception logged) {
            logger.warning("Cannot delete old snapshot data  " + idToString(jobId), logged);
        }
    }

    void cacheValidationRecord(@Nonnull String snapshotMapName, @Nonnull SnapshotValidationRecord validationRecord) {
        exportedSnapshotDetailsCache.set(snapshotMapName, validationRecord);
    }

    public static final class UpdateJobExecutionRecordEntryProcessor implements
                    EntryProcessor<Long, JobExecutionRecord>,
                    EntryBackupProcessor<Long, JobExecutionRecord>,
                    IdentifiedDataSerializable {

        private long jobId;
        @SuppressFBWarnings(value = "SE_BAD_FIELD",
                justification = "this class is not going to be java-serialized")
        private JobExecutionRecord jobExecutionRecord;
        private boolean canCreate;

        public UpdateJobExecutionRecordEntryProcessor() {
        }

        UpdateJobExecutionRecordEntryProcessor(long jobId, JobExecutionRecord jobExecutionRecord, boolean canCreate) {
            this.jobId = jobId;
            this.jobExecutionRecord = jobExecutionRecord;
            this.canCreate = canCreate;
        }

        @Override
        public Object process(Entry<Long, JobExecutionRecord> entry) {
            if (entry.getValue() == null && !canCreate) {
                // ignore missing value - this method of updating cannot be used for initial JobRecord creation
                return "Update to JobRecord for job " + idToString(jobId) + " ignored, oldValue == null";
            }
            if (entry.getValue() != null && entry.getValue().getTimestamp() >= jobExecutionRecord.getTimestamp()) {
                // ignore older update.
                // It can happen because we allow to execute updates in parallel and they can overtake each other.
                // We don't want to overwrite newer update.
                return "Update to JobRecord for job " + idToString(jobId) + " ignored, newer timestamp found. "
                        + "Stored timestamp=" + entry.getValue().getTimestamp() + ", timestamp of the update="
                        + jobExecutionRecord.getTimestamp();
            }
            entry.setValue(jobExecutionRecord);
            return null;
        }

        @Override
        public EntryBackupProcessor<Long, JobExecutionRecord> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Entry<Long, JobExecutionRecord> entry) {
            process(entry);
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.UPDATE_JOB_EXECUTION_RECORD_EP;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(jobId);
            out.writeObject(jobExecutionRecord);
            out.writeBoolean(canCreate);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jobId = in.readLong();
            jobExecutionRecord = in.readObject();
            canCreate = in.readBoolean();
        }
    }

    public static class FilterExecutionIdByJobIdPredicate implements Predicate<Long, Long>, IdentifiedDataSerializable {

        private long jobId;

        public FilterExecutionIdByJobIdPredicate() {
        }

        FilterExecutionIdByJobIdPredicate(long jobId) {
            this.jobId = jobId;
        }

        @Override
        public boolean apply(Entry<Long, Long> mapEntry) {
            return mapEntry.getKey() != jobId && mapEntry.getValue() == jobId;
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.FILTER_EXECUTION_ID_BY_JOB_ID_PREDICATE;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeLong(jobId);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            jobId = in.readLong();
        }
    }

    public static class FilterJobIdPredicate implements Predicate<Long, Long>, IdentifiedDataSerializable {

        public FilterJobIdPredicate() {
        }

        @Override
        public boolean apply(Entry<Long, Long> mapEntry) {
            return mapEntry.getKey().equals(mapEntry.getValue());
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.FILTER_JOB_ID;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    public static class FilterJobRecordByNamePredicate
            implements Predicate<Long, JobRecord>, IdentifiedDataSerializable {

        private String name;

        public FilterJobRecordByNamePredicate() {
        }

        FilterJobRecordByNamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Entry<Long, JobRecord> entry) {
            return name.equals(entry.getValue().getConfig().getName());
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.FILTER_JOB_RECORD_BY_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
        }
    }

    public static class FilterJobResultByNamePredicate
            implements Predicate<Long, JobResult>, IdentifiedDataSerializable {

        private String name;

        public FilterJobResultByNamePredicate() {
        }

        FilterJobResultByNamePredicate(String name) {
            this.name = name;
        }

        @Override
        public boolean apply(Entry<Long, JobResult> entry) {
            return name.equals(entry.getValue().getJobConfig().getName());
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.FILTER_JOB_RESULT_BY_NAME;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeUTF(name);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            name = in.readUTF();
        }
    }
}
