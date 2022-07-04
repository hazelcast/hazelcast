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

package com.hazelcast.jet.impl;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.deployment.ChildFirstClassLoader;
import com.hazelcast.jet.impl.deployment.JetClassLoader;
import com.hazelcast.jet.impl.deployment.JetDelegatingClassLoader;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFine;
import static com.hazelcast.jet.impl.util.LoggingUtil.logFinest;
import static java.util.Collections.unmodifiableMap;

public class JobClassLoaderService {

    // The type of classLoaders field is CHM and not ConcurrentMap because we
    // rely on specific semantics of computeIfAbsent. ConcurrentMap.computeIfAbsent
    // does not guarantee at most one computation per key.
    // key: jobId
    private final ConcurrentHashMap<Long, JobClassLoaders> classLoaders = new ConcurrentHashMap<>();

    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final JobRepository jobRepository;

    public JobClassLoaderService(@Nonnull NodeEngine nodeEngine, @Nonnull JobRepository jobRepository) {
        this.logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.jobRepository = jobRepository;
    }

    public Map<Long, JobClassLoaders> getClassLoaders() {
        return Collections.unmodifiableMap(classLoaders);
    }

    /**
     * Get or create a Job classloader for a job with given config.
     * <p>
     * It also creates processor classloaders if any are configured.
     *
     * @param config job config to use to create the classloader
     * @param jobId  id of the job
     * @param phase  phase for which the classloader is needed - coordinator/member
     * @return job classloader
     */
    public ClassLoader getOrCreateClassLoader(JobConfig config, long jobId, JobPhase phase) {
        JetConfig jetConfig = nodeEngine.getConfig().getJetConfig();
        JobClassLoaders jobClassLoaders = classLoaders.compute(jobId,
                (k, current) -> {
                    JobClassLoaders result = current;
                    if (current == null) {
                        result = createJobClassLoaders(config, jobId, jetConfig);
                    }
                    result.recordPhase(phase);
                    return result;
                }
        );
        jobClassLoaders.recordPhase(phase);
        return jobClassLoaders.jobClassLoader();
    }

    private JobClassLoaders createJobClassLoaders(JobConfig config, long jobId, JetConfig jetConfig) {
        return AccessController.doPrivileged(
                (PrivilegedAction<JobClassLoaders>) () -> {
                    logger.fine("Creating job classLoader for job " + idToString(jobId));
                    ClassLoader parent = parentClassLoader(config);
                    JetDelegatingClassLoader jobClassLoader;
                    if (!jetConfig.isResourceUploadEnabled()) {
                        jobClassLoader = new JetDelegatingClassLoader(parent);
                    } else {
                        jobClassLoader = new JetClassLoader(nodeEngine, parent, config.getName(), jobId,
                                jobRepository);
                    }

                    Map<String, ClassLoader> processorCls = createProcessorClassLoaders(
                            jobId, config, jobClassLoader
                    );
                    return new JobClassLoaders(jobClassLoader, processorCls);
                });
    }

    private ClassLoader parentClassLoader(JobConfig config) {
        // config can be null for light jobs initialized after receiving a packet, but before the
        // InitExecutionOperation was received. We can ignore the classLoaderFactory, because
        // it's not supported anyway for light jobs.
        return config != null && config.getClassLoaderFactory() != null
                ? config.getClassLoaderFactory().getJobClassLoader()
                : nodeEngine.getConfigClassLoader();
    }

    private Map<String, ClassLoader> createProcessorClassLoaders(long jobId, JobConfig jobConfig, ClassLoader parent) {
        logger.fine("Create processor classloader map for job " + idToString(jobId));
        String customLibDir = nodeEngine.getProperties().getString(ClusterProperty.PROCESSOR_CUSTOM_LIB_DIR);
        Map<String, ClassLoader> classLoaderMap = new HashMap<>();
        for (Entry<String, List<String>> entry : jobConfig.getCustomClassPaths().entrySet()) {
            List<URL> list = entry.getValue().stream()
                                  .map(jar -> {
                                      try {
                                          Path path = Paths.get(customLibDir, jar);
                                          return path.toUri().toURL();
                                      } catch (MalformedURLException e) {
                                          throw new JetException(e);
                                      }
                                  })
                                  .collect(Collectors.toList());
            URL[] urls = list.toArray(new URL[]{});
            classLoaderMap.put(entry.getKey(), new ChildFirstClassLoader(urls, parent));
        }
        return unmodifiableMap(classLoaderMap);
    }

    /**
     * Prepare processor classloaders for given job for current thread
     *
     * @param jobId id of the job
     */
    public void prepareProcessorClassLoaders(long jobId) {
        ProcessorClassLoaderTLHolder.putAll(getProcessorClassLoaders(jobId));
    }

    private Map<String, ClassLoader> getProcessorClassLoaders(long jobId) {
        return classLoaders.get(jobId).processorCls();
    }

    /**
     * Clears processor classloaders from the current thread
     */
    public void clearProcessorClassLoaders() {
        ProcessorClassLoaderTLHolder.remove();
    }

    /**
     * Return processor classloader for a vertex with given name, in a job specified by the id
     * <p>
     * This method must be called after the classloader was created by
     * {@link #getOrCreateClassLoader(JobConfig, long, JobPhase)} on this
     * member.
     *
     * @param jobId      job id
     * @param vertexName vertex name
     * @return processor classloader, null if the classloader is not defined for the vertex
     */
    public ClassLoader getProcessorClassLoader(long jobId, String vertexName) {
        JobClassLoaders jobClassLoaders = classLoaders.get(jobId);
        if (jobClassLoaders != null) {
            return jobClassLoaders.processorCl(vertexName);
        } else {
            throw new HazelcastException("JobClassLoaders for jobId=" + Util.idToString(jobId)
                    + " requested, but it does not exists");
        }
    }

    /**
     * Try to remove and close/shutdown job classloader and any processor
     * classloaders for given job
     * <p>
     * We keep track of phases where the classloader is used and remove
     * the classloader only if there are no more phases left.
     */
    public void tryRemoveClassloadersForJob(long jobId, JobPhase phase) {
        logFinest(logger, "Try remove classloaders for jobId=%s, phase=%s", idToString(jobId), phase);
        classLoaders.compute(jobId, (k, jobClassLoaders) -> {
            if (jobClassLoaders == null) {
                logger.warning("JobClassLoaders for jobId=" + idToString(jobId) + " already removed");
                return null;
            }

            int phaseCount = jobClassLoaders.removePhase(phase);
            if (phaseCount == 0) {
                logFinest(logger, "JobClassLoaders phaseCount = 0, removing classloaders for jobId=%s",
                        idToString(jobId));
                Map<String, ClassLoader> processorCls = jobClassLoaders.processorCls();
                if (processorCls != null) {
                    for (ClassLoader cl : processorCls.values()) {
                        try {
                            ((ChildFirstClassLoader) cl).close();
                        } catch (IOException e) {
                            logger.warning("Exception when closing processor classloader", e);
                        }
                    }
                }
                // the class loader might not have been initialized if the job failed before that
                JetDelegatingClassLoader jobClassLoader = jobClassLoaders.jobClassLoader();
                jobClassLoader.shutdown();

                logFine(logger, "Finish JobClassLoaders phaseCount = 0," +
                        " removing classloaders for jobId=%s", idToString(jobId));
                // Removes the item from the map
                return null;
            } else {
                logFinest(logger, "JobClassLoaders refCount > 0, NOT removing classloaders for jobId=%s", idToString(jobId));
                return jobClassLoaders;
            }
        });
    }

    /**
     * Returns the job classloader for the job with given id.
     *
     * @param jobId job id
     * @return the job classloader, null if the classloader hasn't been created yet or was already destroyed
     */
    public JetDelegatingClassLoader getClassLoader(long jobId) {
        JobClassLoaders jobClassLoaders = classLoaders.get(jobId);
        return jobClassLoaders == null ? null : jobClassLoaders.jobClassLoader;
    }

    /**
     * Phase where the classloader is needed
     */
    public enum JobPhase {

        /**
         * Needed on job coordinator (master) for Pipeline/DAG deserialization and MetaSupplier init/get/close
         */
        COORDINATOR,

        /**
         * Needed on member for ExecutionPlan deserialization, ProcessorSupplier init/get/close and Processors
         */
        EXECUTION
    }

    /**
     * Keeps job classloader and potentially processor classloaders for a job.
     *
     * Note:
     * On master node there is a race between closing PMS and PS.
     * We need to close the classloader only after both have been called.
     * When we create JobClassLoaders we record that by calling {@link #recordPhase(JobPhase)} and
     * when we release it we call {@link #removePhase(JobPhase)}
     */
    private static class JobClassLoaders {

        private final JetDelegatingClassLoader jobClassLoader;
        private final Map<String, ClassLoader> processorCls;

        // EnumSet is not thread-safe
        // Interactions are synchronized on this. Its reference never leaks the JobClassLoaders instance.
        private final EnumSet<JobPhase> phases = EnumSet.noneOf(JobPhase.class);

        JobClassLoaders(
                @Nonnull JetDelegatingClassLoader jobClassLoader,
                @Nonnull Map<String, ClassLoader> processorCls
        ) {
            this.jobClassLoader = jobClassLoader;
            this.processorCls = unmodifiableMap(processorCls);
        }

        public JetDelegatingClassLoader jobClassLoader() {
            return jobClassLoader;
        }

        public Map<String, ClassLoader> processorCls() {
            return processorCls;
        }

        public ClassLoader processorCl(String key) {
            return processorCls.get(key);
        }

        public void recordPhase(JobPhase phase) {
            synchronized (this) {
                phases.add(phase);
            }
        }

        public int removePhase(JobPhase phase) {
            synchronized (this) {
                phases.remove(phase);
                return phases.size();
            }
        }

        @Override
        public String toString() {
            synchronized (this) {
                return "JobClassLoaders{" +
                        "phases=" + phases +
                        '}';
            }
        }
    }
}
