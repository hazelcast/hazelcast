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

package com.hazelcast.jet.impl.coordination;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.impl.JobRecord;
import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.Predicate;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
import java.util.zip.DeflaterOutputStream;

import static com.hazelcast.jet.config.JetConfig.IDS_MAP_NAME;
import static com.hazelcast.jet.config.JetConfig.JOB_RECORDS_MAP_NAME;
import static com.hazelcast.jet.config.JetConfig.RESOURCES_MAP_NAME_PREFIX;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static java.util.concurrent.TimeUnit.HOURS;

public class JobRepository {

    private static final String RESOURCE_MARKER = "__jet.jobId";
    private static final long JOB_EXPIRATION_DURATION_IN_MILLIS = HOURS.toMillis(2);

    private final HazelcastInstance instance;
    private final IMap<Long, Long> jobIds;
    private final IMap<Long, JobRecord> jobs;
    private long jobExpirationDurationInMillis = JOB_EXPIRATION_DURATION_IN_MILLIS;

    public JobRepository(JetInstance jetInstance) {
        this.instance = jetInstance.getHazelcastInstance();
        this.jobIds = instance.getMap(IDS_MAP_NAME);
        this.jobs = instance.getMap(JOB_RECORDS_MAP_NAME);
    }

    void setJobExpirationDurationInMillis(long jobExpirationDurationInMillis) {
        this.jobExpirationDurationInMillis = jobExpirationDurationInMillis;
    }

    /**
     * Uploads job resources and returns a unique job id generated for the job.
     * If the upload process fails for any reason, such as being unable to access to a resource,
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
                    cleanupJobResourcesMap(jobResourcesMap);
                    jobIds.remove(jobId);
                    throw new JetException("Job resource upload failed", e);
                }
            } else {
                try {
                    InputStream in = rc.getUrl().openStream();
                    readStreamAndPutCompressedToMap(rc.getId(), tmpMap, in);
                } catch (IOException e) {
                    cleanupJobResourcesMap(jobResourcesMap);
                    jobIds.remove(jobId);
                    throw new JetException("Job resource upload failed", e);
                }
            }

            // now upload it all
            jobResourcesMap.putAll(tmpMap);
        }

        // the marker object will be used to decide when to clean up job resources
        jobResourcesMap.put(RESOURCE_MARKER, jobId);

        return jobId;
    }

    private long newJobId() {
        long jobId;
        do {
            jobId = Util.secureRandomNextLong();
        } while (jobIds.putIfAbsent(jobId, jobId) != null);
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

    private void cleanupJobResourcesMap(IMap<String, Object> jobResourcesMap) {
        jobResourcesMap.clear();
        jobResourcesMap.destroy();
    }

    /**
     * Puts the given job record into the jobs map.
     * If another job record is already put, it checks if it has the same DAG.
     * If it has a different DAG, then the call fails with {@link IllegalStateException}
     */
    void putNewJobRecord(JobRecord jobRecord) {
        long jobId = jobRecord.getJobId();
        JobRecord prev = jobs.putIfAbsent(jobId, jobRecord);
        if (prev != null && !prev.getDag().equals(jobRecord.getDag())) {
            throw new IllegalStateException("Cannot put job record for job " + idToString(jobId)
                    + " because it already exists with a different dag");
        }
    }

    /**
     * Generates a new execution id for the given job id, guaranteed to be unique across the cluster
     */
    long newExecutionId(long jobId) {
        long executionId;
        do {
            executionId = Util.secureRandomNextLong();
        } while (jobIds.putIfAbsent(executionId, jobId) != null);
        return executionId;
    }

    /**
     * Returns the job record creation time for the given job id.
     * If the job record is not found, fails with an {@link IllegalArgumentException}
     */
    long getJobCreationTimeOrFail(long jobId) {
        EntryView<Long, JobRecord> entryView = jobs.getEntryView(jobId);
        if (entryView != null) {
            return entryView.getCreationTime();
        }

        throw new IllegalArgumentException("Job creation time not found for job id: " + idToString(jobId));
    }

    /**
     * Performs cleanup after job completion. Deletes job record and job resources but keeps the job id
     * so that it will not be used again for a new job submission
     */
    void deleteJob(long jobId) {
        // Delete the job record
        jobs.remove(jobId);
        // Delete the execution ids, but keep the job id
        Set<Long> executionIds = jobIds.keySet(new FilterExecutionIdByJobIdPredicate(jobId));
        executionIds.forEach(jobIds::remove);

        // Delete job resources
        cleanupJobResourcesMap(getJobResources(jobId));
    }

    /**
     * Cleans up stale job records, execution ids and job resources.
     */
    void cleanup(Set<Long> completedJobIds, Set<Long> runningJobIds) {
        // clean up completed jobs
        completedJobIds.forEach(this::deleteJob);

        Set<Long> validJobIds = new HashSet<>();
        validJobIds.addAll(completedJobIds);
        validJobIds.addAll(runningJobIds);
        validJobIds.addAll(jobs.keySet());

        // Job ids are never cleaned up.
        // We also don't clean up job records here because they might be started in parallel while cleanup is running
        // If a job id is not running or completed, it might be suitable for job resource clean up
        jobIds.keySet(new FilterJobIdPredicate())
              .stream()
              .filter(jobId -> !validJobIds.contains(jobId))
              .forEach(jobId -> {
                  IMap<String, Object> resources = getJobResources(jobId);
                  if (resources.isEmpty()) {
                      return;
                  }

                  EntryView<String, Object> marker = resources.getEntryView(RESOURCE_MARKER);
                  // If the marker is absent, then job resources may be still uploaded.
                  // Just put the marker so that the job resources may be cleaned up eventually.
                  // If the job resources are still being uploaded, then the marker will be overwritten, which is ok.
                  if (marker == null) {
                      resources.putIfAbsent(RESOURCE_MARKER, RESOURCE_MARKER);
                  } else if (isJobExpired(marker.getCreationTime())) {
                      cleanupJobResourcesMap(resources);
                  }
              });
    }

    private boolean isJobExpired(long creationTime) {
        return (System.currentTimeMillis() - creationTime) >= jobExpirationDurationInMillis;
    }

    Collection<JobRecord> getJobRecords() {
        return jobs.values();
    }

    public JobRecord getJob(long jobId) {
       return jobs.get(jobId);
    }

    <T> IMap<String, T> getJobResources(long jobId) {
        return instance.getMap(RESOURCES_MAP_NAME_PREFIX + jobId);
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
            return JetImplDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetImplDataSerializerHook.FILTER_EXECUTION_ID_BY_JOB_ID_PREDICATE;
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
            return JetImplDataSerializerHook.FACTORY_ID;
        }

        @Override
        public int getId() {
            return JetImplDataSerializerHook.FILTER_JOB_ID;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

    }

}
