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

package com.hazelcast.jet.impl;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;

import javax.annotation.Nullable;
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

import static com.hazelcast.jet.Jet.INTERNAL_JET_OBJECTS_PREFIX;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static java.util.Collections.newSetFromMap;
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.stream.Collectors.toList;

public class JobRepository {

    /**
     * Name of internal IMap which stores job resources
     */
    public static final String RESOURCES_MAP_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "resources.";

    /**
     * Name of internal IMap which is used for unique id generation
     */
    public static final String RANDOM_IDS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "ids";

    /**
     * Name of internal IMap which stores job records
     */
    public static final String JOB_RECORDS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "records";

    /**
     * Name of internal IMap which stores job results
     */
    public static final String JOB_RESULTS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "results";

    private static final String RESOURCE_MARKER = "__jet.resourceMarker";
    private static final long DEFAULT_RESOURCES_EXPIRATION_MILLIS = HOURS.toMillis(2);

    private final HazelcastInstance instance;
    private final SnapshotRepository snapshotRepository;

    private final IMap<Long, Long> randomIds;
    private final IMap<Long, JobRecord> jobRecords;
    private final IMap<Long, JobResult> jobResults;
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

    /**
     * @param snapshotRepository Can be {@code null} if used on client to upload resources.
     */
    JobRepository(JetInstance jetInstance, @Nullable SnapshotRepository snapshotRepository) {
        this.instance = jetInstance.getHazelcastInstance();
        this.snapshotRepository = snapshotRepository;

        this.randomIds = instance.getMap(RANDOM_IDS_MAP_NAME);
        this.jobRecords = instance.getMap(JOB_RECORDS_MAP_NAME);
        this.jobResults = instance.getMap(JOB_RESULTS_MAP_NAME);
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
        if (snapshotRepository != null) {
            snapshotRepository.deleteAllSnapshots(jobId);
        }
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
                    + " because it already exists with a different dag");
        }
    }

    /**
     * Updates the job quorum size if it is only larger than the current quorum size of the given job
     */
    boolean updateJobQuorumSizeIfLargerThanCurrent(long jobId, int newQuorumSize) {
        return (boolean) jobRecords.executeOnKey(jobId, new UpdateJobRecordQuorumEntryProcessor(newQuorumSize));
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
        JobResult jobResult = new JobResult(jobId, config, coordinator, creationTime, completionTime, error);

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
    private void deleteJob(long jobId) {
        if (deletedJobs.contains(jobId)) {
            return;
        }

        // Delete the job record
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
        // We also don't clean up job records here because they might be started in parallel while cleanup is running
        // If a job id is not running or is completed it might be suitable to clean up job resources
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

    <T> IMap<String, T> getJobResources(long jobId) {
        return instance.getMap(RESOURCES_MAP_NAME_PREFIX + idToString(jobId));
    }

    public JobResult getJobResult(long jobId) {
        return jobResults.get(jobId);
    }

    List<JobResult> getJobResults(String name) {
        return jobResults.values(new FilterJobResultByNamePredicate(name)).stream()
                  .sorted(comparing(JobResult::getCreationTime).reversed()).collect(toList());
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

    public static class UpdateJobRecordQuorumEntryProcessor
            implements EntryProcessor<Long, JobRecord>, IdentifiedDataSerializable {

        private int newQuorumSize;
        private boolean updated;

        public UpdateJobRecordQuorumEntryProcessor() {
        }

        UpdateJobRecordQuorumEntryProcessor(int newQuorumSize) {
            this.newQuorumSize = newQuorumSize;
        }

        @Override
        public Object process(Entry<Long, JobRecord> entry) {
            JobRecord jobRecord = entry.getValue();
            if (jobRecord == null) {
                return false;
            }

            updated = (newQuorumSize > jobRecord.getQuorumSize());
            if (updated) {
                JobRecord newJobRecord = new JobRecord(jobRecord.getJobId(), jobRecord.getCreationTime(),
                        jobRecord.getDag(), jobRecord.getConfig(), newQuorumSize);
                entry.setValue(newJobRecord);
            }

            return updated;
        }

        @Override
        public EntryBackupProcessor<Long, JobRecord> getBackupProcessor() {
            return updated ? new UpdateJobRecordQuorumEntryBackupProcessor(newQuorumSize) : null;
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.UPDATE_JOB_QUORUM;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(newQuorumSize);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            newQuorumSize = in.readInt();
        }
    }

    public static class UpdateJobRecordQuorumEntryBackupProcessor
            implements EntryBackupProcessor<Long, JobRecord>, IdentifiedDataSerializable {

        private int newQuorumSize;

        public UpdateJobRecordQuorumEntryBackupProcessor() {
        }

        UpdateJobRecordQuorumEntryBackupProcessor(int newQuorumSize) {
            this.newQuorumSize = newQuorumSize;
        }

        @Override
        public void processBackup(Entry<Long, JobRecord> entry) {
            JobRecord jobRecord = entry.getValue();
            if (jobRecord == null) {
                return;
            }

            JobRecord newJobRecord = new JobRecord(jobRecord.getJobId(), jobRecord.getCreationTime(),
                    jobRecord.getDag(), jobRecord.getConfig(), newQuorumSize);
            entry.setValue(newJobRecord);
        }

        @Override
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetInitDataSerializerHook.UPDATE_JOB_QUORUM_BACKUP;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(newQuorumSize);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            newQuorumSize = in.readInt();
        }
    }

    public static class FilterJobRecordByNamePredicate
            implements Predicate<Long, JobRecord>, IdentifiedDataSerializable {

        private String name;

        public FilterJobRecordByNamePredicate() {
        }

        public FilterJobRecordByNamePredicate(String name) {
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

        public FilterJobResultByNamePredicate(String name) {
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
