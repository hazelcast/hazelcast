/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.core.JetProperties;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.deployment.IMapOutputStream;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.impl.util.ImdgUtil;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.spi.impl.NodeEngine;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.stream.Collectors;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static com.hazelcast.jet.Util.idFromString;
import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.IOUtil.fileNameFromUrl;
import static com.hazelcast.jet.impl.util.IOUtil.packDirectoryIntoZip;
import static com.hazelcast.jet.impl.util.IOUtil.packStreamIntoZip;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static java.util.Comparator.comparing;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
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
     * Name of internal IMap which stores job resources and attached files.
     */
    public static final String RESOURCES_MAP_NAME_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "resources.";

    /**
     * Key prefix for attached job files in the IMap named {@link JobRepository#RESOURCES_MAP_NAME_PREFIX}.
     */
    public static final String FILE_STORAGE_KEY_NAME_PREFIX = "f.";

    /**
     * Key prefix for added class resources in the IMap named {@link JobRepository#RESOURCES_MAP_NAME_PREFIX}.
     */
    public static final String CLASS_STORAGE_KEY_NAME_PREFIX = "c.";

    /**
     * Name of internal flake ID generator which is used for unique id generation.
     */
    public static final String RANDOM_ID_GENERATOR_NAME = INTERNAL_JET_OBJECTS_PREFIX + "ids";

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
     * Name of internal IMap which stores {@link JobMetrics}s.
     */
    public static final String JOB_METRICS_MAP_NAME = INTERNAL_JET_OBJECTS_PREFIX + "results.metrics";

    /**
     * Prefix for internal IMaps which store snapshot data. Snapshot data for
     * one snapshot is stored in either of the following two maps:
     * <ul>
     *      <li>{@code _jet.snapshot.<jobId>.0}
     *      <li>{@code _jet.snapshot.<jobId>.1}
     * </ul>
     * Which one of these is determined in {@link JobExecutionRecord}.
     */
    public static final String SNAPSHOT_DATA_MAP_PREFIX = INTERNAL_JET_OBJECTS_PREFIX + "snapshot.";

    /**
     * Only do the cleanup if the number of JobResults exceeds the maximum
     * number by at least 5% (1/20 = 0.05 = 5%).
     */
    private static final int MAX_NO_RESULTS_OVERHEAD = 20;
    private static final long DEFAULT_RESOURCES_EXPIRATION_MILLIS = HOURS.toMillis(2);
    private static final int JOB_ID_STRING_LENGTH = idToString(0L).length();


    private final HazelcastInstance instance;
    private final ILogger logger;

    private final IMap<Long, JobRecord> jobRecords;
    private final IMap<Long, JobExecutionRecord> jobExecutionRecords;
    private final IMap<Long, JobResult> jobResults;
    private final IMap<Long, List<RawJobMetrics>> jobMetrics;
    private final IMap<String, SnapshotValidationRecord> exportedSnapshotDetailsCache;
    private final FlakeIdGenerator idGenerator;

    private long resourcesExpirationMillis = DEFAULT_RESOURCES_EXPIRATION_MILLIS;

    public JobRepository(JetInstance jetInstance) {
        this.instance = jetInstance.getHazelcastInstance();
        this.logger = instance.getLoggingService().getLogger(getClass());

        this.idGenerator = instance.getFlakeIdGenerator(RANDOM_ID_GENERATOR_NAME);
        this.jobRecords = instance.getMap(JOB_RECORDS_MAP_NAME);
        this.jobExecutionRecords = instance.getMap(JOB_EXECUTION_RECORDS_MAP_NAME);
        this.jobResults = instance.getMap(JOB_RESULTS_MAP_NAME);
        this.jobMetrics = instance.getMap(JOB_METRICS_MAP_NAME);
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
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    long uploadJobResources(long jobId, JobConfig jobConfig) {
        Map<String, byte[]> tmpMap = new HashMap<>();
        try {
            Supplier<IMap<String, byte[]>> jobFileStorage = Util.memoize(() -> getJobResources(jobId));
            for (ResourceConfig rc : jobConfig.getResourceConfigs().values()) {
                switch (rc.getResourceType()) {
                    case CLASSPATH_RESOURCE:
                    case CLASS:
                        try (InputStream in = rc.getUrl().openStream()) {
                            readStreamAndPutCompressedToMap(rc.getId(), tmpMap, in);
                        }
                        break;
                    case FILE:
                        try (InputStream in = rc.getUrl().openStream();
                             IMapOutputStream os = new IMapOutputStream(jobFileStorage.get(), fileKeyName(rc.getId()))
                        ) {
                            packStreamIntoZip(in, os, requireNonNull(fileNameFromUrl(rc.getUrl())));
                        }
                        break;
                    case DIRECTORY:
                        Path baseDir = validateAndGetDirectoryPath(rc);
                        try (IMapOutputStream os = new IMapOutputStream(jobFileStorage.get(), fileKeyName(rc.getId()))) {
                            packDirectoryIntoZip(baseDir, os);
                        }
                        break;
                    case JAR:
                        loadJar(tmpMap, rc);
                        break;
                    case JARS_IN_ZIP:
                        loadJarsInZip(tmpMap, rc.getUrl());
                        break;
                    default:
                        throw new JetException("Unsupported resource type: " + rc.getResourceType());
                }
            }
        } catch (IOException | URISyntaxException e) {
            throw new JetException("Job resource upload failed", e);
        }
        // avoid creating resources map if map is empty
        if (tmpMap.size() > 0) {
            IMap<String, byte[]> jobResourcesMap = getJobResources(jobId);
            // now upload it all
            try {
                jobResourcesMap.putAll(tmpMap);
            } catch (Exception e) {
                jobResourcesMap.destroy();
                throw new JetException("Job resource upload failed", e);
            }
        }
        return jobId;
    }

    private Path validateAndGetDirectoryPath(ResourceConfig rc) throws URISyntaxException, IOException {
        Path baseDir = Paths.get(rc.getUrl().toURI());
        if (!Files.isDirectory(baseDir)) {
            throw new FileNotFoundException(baseDir + " is not a valid directory");
        }
        return baseDir;
    }

    public long newJobId() {
        return idGenerator.newId();
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "it's a false positive since java 11: https://github.com/spotbugs/spotbugs/issues/756")
    private void loadJar(Map<String, byte[]> tmpMap, ResourceConfig rc) throws IOException {
        try (InputStream in = rc.getUrl().openStream()) {
            loadJarFromInputStream(tmpMap, in);
        }
    }

    /**
     * Unzips the ZIP archive and processes JAR files
     */
    private void loadJarsInZip(Map<String, byte[]> map, URL url) throws IOException {
        try (ZipInputStream zis = new ZipInputStream(new BufferedInputStream(url.openStream()))) {
            ZipEntry zipEntry;
            while ((zipEntry = zis.getNextEntry()) != null) {
                if (zipEntry.isDirectory()) {
                    continue;
                }
                if (zipEntry.getName().toLowerCase().endsWith(".jar")) {
                    loadJarFromInputStream(map, zis);
                }
            }
        }
    }

    /**
     * Unzips the JAR archive and processes individual entries using
     * {@link #readStreamAndPutCompressedToMap(String, Map, InputStream)}.
     */
    private void loadJarFromInputStream(Map<String, byte[]> map, InputStream is) throws IOException {
        JarInputStream jis = new JarInputStream(is);
        JarEntry jarEntry;
        while ((jarEntry = jis.getNextJarEntry()) != null) {
            if (jarEntry.isDirectory()) {
                continue;
            }
            readStreamAndPutCompressedToMap(jarEntry.getName(), map, jis);
        }
    }

    private void readStreamAndPutCompressedToMap(
            String resourceName, Map<String, byte[]> map, InputStream in
    ) throws IOException {
        // ignore duplicates: the first resource in first jar takes precedence
        if (map.containsKey(resourceName)) {
            return;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DeflaterOutputStream compressor = new DeflaterOutputStream(baos)) {
            IOUtil.drainTo(in, compressor);
        }

        map.put(classKeyName(resourceName), baos.toByteArray());
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
        jobExecutionRecords.executeOnKey(jobId, ImdgUtil.entryProcessor((key, value) -> {
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
    long newExecutionId() {
        return idGenerator.newId();
    }

    /**
     * Puts a JobResult for the given job and deletes the JobRecord.
     *
     * @throws JobNotFoundException  if the JobRecord is not found
     * @throws IllegalStateException if the JobResult is already present
     */
    void completeJob(
            @Nonnull MasterContext masterContext,
            @Nullable List<RawJobMetrics> terminalMetrics,
            @Nullable Throwable error,
            long completionTime
    ) {
        long jobId = masterContext.jobId();

        JobConfig config = masterContext.jobRecord().getConfig();
        long creationTime = masterContext.jobRecord().getCreationTime();
        JobResult jobResult = new JobResult(jobId, config, creationTime, completionTime, toErrorMsg(error));

        if (terminalMetrics != null) {
            try {
                List<RawJobMetrics> prevMetrics = jobMetrics.put(jobId, terminalMetrics);
                if (prevMetrics != null) {
                    logger.warning("Overwriting job metrics for job " + jobResult);
                }
            } catch (Exception e) {
                logger.warning("Storing the job metrics failed, ignoring: " + e, e);
            }
        }
        for (;;) {
            // keep trying to store the JobResult until it succeeds
            try {
                jobResults.set(jobId, jobResult);
                break;
            } catch (Exception e) {
                // if the local instance was shut down, re-throw the error
                if (e instanceof HazelcastInstanceNotActiveException && (!instance.getLifecycleService().isRunning())) {
                    throw e;
                }
                // retry otherwise, after a delay
                long retryTimeoutSeconds = 1;
                logger.warning("Failed to store JobResult, will retry in " + retryTimeoutSeconds + " seconds: " + e, e);
                LockSupport.parkNanos(SECONDS.toNanos(retryTimeoutSeconds));
            }
        }

        deleteJob(jobId);
    }

    /**
     * Performs cleanup after job completion. Deletes job record and job resources but keeps the job id
     * so that it will not be used again for a new job submission.
     */
    void deleteJob(long jobId) {
        // delete the job record and related records
        // ignore the eventual failure - there's a separate cleanup process that will take care
        BiConsumer<Object, Throwable> callback = (v, t) -> {
            if (t != null) {
                logger.warning("Failed to remove " + v.getClass().getSimpleName() + " for job "
                            + idToString(jobId) + ", ignoring", t);
            }
        };
        jobExecutionRecords.removeAsync(jobId).whenComplete(callback);
        jobRecords.removeAsync(jobId).whenComplete(callback);
    }

    /**
     * Cleans up stale maps related to jobs
     */
    void cleanup(NodeEngine nodeEngine) {
        long start = System.nanoTime();

        cleanupMaps(nodeEngine);
        cleanupJobResults(nodeEngine);

        long elapsed = System.nanoTime() - start;
        logger.fine("Job cleanup took " + TimeUnit.NANOSECONDS.toMillis(elapsed) + "ms");
    }

    private void cleanupMaps(NodeEngine nodeEngine) {
        Collection<DistributedObject> maps =
                nodeEngine.getProxyService().getDistributedObjects(MapService.SERVICE_NAME);

        // we need to take the list of active job records after getting the list of maps --
        // otherwise the job records could be missing newly submitted jobs
        Set<Long> activeJobs = jobRecords.keySet();

        for (DistributedObject map : maps) {
            if (map.getName().startsWith(SNAPSHOT_DATA_MAP_PREFIX)) {
                long id = jobIdFromPrefixedName(map.getName(), SNAPSHOT_DATA_MAP_PREFIX);
                if (!activeJobs.contains(id)) {
                    logFine(logger, "Deleting snapshot data map '%s' because job already finished", map.getName());
                    map.destroy();
                }
            } else if (map.getName().startsWith(RESOURCES_MAP_NAME_PREFIX)) {
                deleteMap(activeJobs, map);
            }
        }
    }

    private void deleteMap(Set<Long> activeJobs, DistributedObject map) {
        long id = jobIdFromPrefixedName(map.getName(), RESOURCES_MAP_NAME_PREFIX);
        if (activeJobs.contains(id)) {
            // job is still active, do nothing
            return;
        }
        if (jobResults.containsKey(id)) {
            // if job is finished, we can safely delete the map
            logFine(logger, "Deleting job resource map '%s' because job is already finished", map.getName());
            map.destroy();
        } else {
            // Job might be in the process of uploading resources, check how long the map has been there.
            // If we happen to recreate a just-deleted map, it will be destroyed again after
            // resourcesExpirationMillis.
            @SuppressWarnings("rawtypes")
            IMap resourceMap = (IMap) map;
            long creationTime = resourceMap.getLocalMapStats().getCreationTime();
            if (isResourceMapExpired(creationTime)) {
                logger.fine("Deleting job resource map " + map.getName() + " because the map " +
                        "was created long ago and job record or result still doesn't exist");
                resourceMap.destroy();
            }
        }
    }

    private void cleanupJobResults(NodeEngine nodeEngine) {
        int maxNoResults = Math.max(1, nodeEngine.getProperties().getInteger(JetProperties.JOB_RESULTS_MAX_SIZE));
        // delete oldest job results
        if (jobResults.size() > Util.addClamped(maxNoResults, maxNoResults / MAX_NO_RESULTS_OVERHEAD)) {
            jobResults.values().stream().sorted(comparing(JobResult::getCompletionTime).reversed())
                    .skip(maxNoResults)
                    .map(JobResult::getJobId)
                    .collect(Collectors.toList())
                    .forEach(id -> {
                        jobMetrics.delete(id);
                        jobResults.delete(id);
                    });
        }
    }

    private static String toErrorMsg(@Nullable Throwable error) {
        if (error == null) {
            return null;
        }
        if (error.getClass().equals(JetException.class) && error.getMessage() != null) {
            String stackTrace = ExceptionUtil.stackTraceToString(error);
            // The error message is later thrown as JetException
            // Remove leading 'com.hazelcast.jet.JetException: ' from the stack trace to avoid double JetException
            // in the final stacktrace
            return stackTrace.substring(stackTrace.indexOf(' ') + 1);
        }
        return ExceptionUtil.stackTraceToString(error);
    }

    private static long jobIdFromPrefixedName(String name, String prefix) {
        int idx = prefix.length();
        String jobId = name.substring(idx, idx + JOB_ID_STRING_LENGTH);
        return idFromString(jobId);
    }

    private boolean isResourceMapExpired(long creationTime) {
        return (System.currentTimeMillis() - creationTime) >= resourcesExpirationMillis;
    }

    Set<Long> getAllJobIds() {
        Set<Long> ids = new HashSet<>();
        ids.addAll(jobRecords.keySet());
        ids.addAll(jobResults.keySet());
        return ids;
    }

    public Collection<JobRecord> getJobRecords() {
        return jobRecords.values();
    }

    public JobRecord getJobRecord(long jobId) {
        return jobRecords.get(jobId);
    }

    public JobExecutionRecord getJobExecutionRecord(long jobId) {
        return jobExecutionRecords.get(jobId);
    }

    /**
     * Gets the job resources map
     */
    public IMap<String, byte[]> getJobResources(long jobId) {
        return instance.getMap(jobResourcesMapName(jobId));
    }

    @Nullable
    public JobResult getJobResult(long jobId) {
        return jobResults.get(jobId);
    }

    @Nullable
    List<RawJobMetrics> getJobMetrics(long jobId) {
        return jobMetrics.get(jobId);
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
     * Returns the map name in the form {@code __jet.resources.<jobId>}
     */
    public static String jobResourcesMapName(long jobId) {
        return RESOURCES_MAP_NAME_PREFIX + idToString(jobId);
    }

    /**
     * Returns the key name in the form {@code file.<id>}
     */
    public static String fileKeyName(String id) {
        return FILE_STORAGE_KEY_NAME_PREFIX + id;
    }

    /**
     * Returns the key name in the form {@code class.<id>}
     */
    public static String classKeyName(String id) {
        return CLASS_STORAGE_KEY_NAME_PREFIX + id;
    }

    /**
     * Returns map name in the form {@code "_jet.exportedSnapshot.<jobId>.<dataMapIndex>"}.
     */
    public static String exportedSnapshotMapName(String name) {
        return JobRepository.EXPORTED_SNAPSHOTS_PREFIX + name;
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

    void cacheValidationRecord(@Nonnull String snapshotName, @Nonnull SnapshotValidationRecord validationRecord) {
        try {
            exportedSnapshotDetailsCache.set(snapshotName, validationRecord);
        } catch (Exception e) {
            logger.warning("Snapshot name: '" + snapshotName + "', failed to store validation record to cache: " + e, e);
        }
    }

    public static final class UpdateJobExecutionRecordEntryProcessor implements
            EntryProcessor<Long, JobExecutionRecord, Object>,
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
        public int getFactoryId() {
            return JetInitDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getClassId() {
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
        public int getClassId() {
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
