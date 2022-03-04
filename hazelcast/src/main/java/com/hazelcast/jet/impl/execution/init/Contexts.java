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

package com.hazelcast.jet.impl.execution.init;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ManagedContext;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.config.ResourceConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.impl.deployment.IMapInputStream;
import com.hazelcast.jet.impl.metrics.MetricsContext;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.jet.Util.idToString;
import static com.hazelcast.jet.config.ResourceType.DIRECTORY;
import static com.hazelcast.jet.config.ResourceType.FILE;
import static com.hazelcast.jet.impl.JobRepository.fileKeyName;
import static com.hazelcast.jet.impl.JobRepository.jobResourcesMapName;
import static com.hazelcast.jet.impl.util.IOUtil.fileNameFromUrl;
import static com.hazelcast.jet.impl.util.IOUtil.unzip;
import static com.hazelcast.jet.impl.util.Util.editPermissionsRecursively;
import static java.lang.Math.min;
import static java.nio.file.attribute.PosixFilePermission.OWNER_WRITE;
import static java.util.Objects.requireNonNull;

public final class Contexts {

    private static final ThreadLocal<Container> CONTEXT = ThreadLocal.withInitial(Container::new);

    private Contexts() {
    }

    public static class MetaSupplierCtx implements ProcessorMetaSupplier.Context {

        private final NodeEngineImpl nodeEngine;
        private final long jobId;
        private final long executionId;
        private final JobConfig jobConfig;
        private final ILogger logger;
        private final String vertexName;
        private final int localParallelism;
        private final int totalParallelism;
        private final int memberCount;
        private final boolean isLightJob;
        private final Map<Address, int[]> partitionAssignment;
        private final Subject subject;
        private final ClassLoader classLoader;

        @SuppressWarnings("checkstyle:ParameterNumber")
        MetaSupplierCtx(
                NodeEngineImpl nodeEngine,
                long jobId,
                long executionId,
                JobConfig jobConfig,
                ILogger logger,
                String vertexName,
                int localParallelism,
                int totalParallelism,
                int memberCount,
                boolean isLightJob,
                Map<Address, int[]> partitionAssignment,
                Subject subject,
                ClassLoader classLoader
        ) {
            this.nodeEngine = nodeEngine;
            this.jobId = jobId;
            this.executionId = executionId;
            this.jobConfig = jobConfig;
            this.logger = logger;
            this.vertexName = vertexName;
            this.totalParallelism = totalParallelism;
            this.localParallelism = localParallelism;
            this.memberCount = memberCount;
            this.isLightJob = isLightJob;
            this.partitionAssignment = partitionAssignment;
            this.subject = subject;
            this.classLoader = classLoader;
        }

        public NodeEngineImpl nodeEngine() {
            return nodeEngine;
        }

        public ClassLoader getClassLoader() {
            return classLoader;
        }

        public Subject subject() {
            return subject;
        }

        @Nonnull @Override
        public HazelcastInstance hazelcastInstance() {
            return nodeEngine.getHazelcastInstance();
        }

        @Nonnull @Override
        @Deprecated
        public JetInstance jetInstance() {
            return (JetInstance) hazelcastInstance().getJet();
        }

        @Override
        public long jobId() {
            return jobId;
        }

        @Override
        public long executionId() {
            return executionId;
        }

        @Nonnull @Override
        public JobConfig jobConfig() {
            return jobConfig;
        }

        @Override
        public int totalParallelism() {
            return totalParallelism;
        }

        @Override
        public int localParallelism() {
            return localParallelism;
        }

        @Override
        public int memberCount() {
            return memberCount;
        }

        @Nonnull @Override
        public String vertexName() {
            return vertexName;
        }

        @Nonnull @Override
        public ILogger logger() {
            return logger;
        }

        @Override
        public ProcessingGuarantee processingGuarantee() {
            return jobConfig.getProcessingGuarantee();
        }

        @Override
        public long maxProcessorAccumulatedRecords() {
            long jobMaxProcessorAccumulatedRecords = jobConfig.getMaxProcessorAccumulatedRecords();
            return jobMaxProcessorAccumulatedRecords > -1
                    ? jobMaxProcessorAccumulatedRecords
                    : hazelcastInstance().getConfig().getJetConfig().getMaxProcessorAccumulatedRecords();
        }

        @Override
        public boolean isLightJob() {
            return isLightJob;
        }

        @Override
        public Map<Address, int[]> partitionAssignment() {
            return partitionAssignment;
        }

        @Override
        public ClassLoader classLoader() {
            return classLoader;
        }
    }

    public static class ProcSupplierCtx extends MetaSupplierCtx implements ProcessorSupplier.Context {

        private final int memberIndex;
        private final ConcurrentHashMap<String, File> tempDirectories;
        private final InternalSerializationService serializationService;

        @SuppressWarnings("checkstyle:ParameterNumber")
        ProcSupplierCtx(
                NodeEngineImpl nodeEngine,
                long jobId,
                long executionId,
                JobConfig jobConfig,
                ILogger logger,
                String vertexName,
                int localParallelism,
                int totalParallelism,
                int memberIndex,
                int memberCount,
                boolean isLightJob,
                Map<Address, int[]> partitionAssignment,
                ConcurrentHashMap<String, File> tempDirectories,
                InternalSerializationService serializationService,
                Subject subject,
                ClassLoader classLoader
        ) {
            super(nodeEngine, jobId, executionId, jobConfig, logger, vertexName, localParallelism, totalParallelism,
                    memberCount, isLightJob, partitionAssignment, subject, classLoader);
            this.memberIndex = memberIndex;
            this.tempDirectories = tempDirectories;
            this.serializationService = serializationService;
        }

        @Override
        public int memberIndex() {
            return memberIndex;
        }

        @Nonnull @Override
        public File attachedDirectory(@Nonnull String id) {
            Preconditions.checkHasText(id, "id cannot be null or empty");
            ResourceConfig resourceConfig = jobConfig().getResourceConfigs().get(id);
            if (resourceConfig == null) {
                throw new JetException(String.format("No resource is attached with ID '%s'", id));
            }
            if (resourceConfig.getResourceType() != DIRECTORY) {
                throw new JetException(String.format(
                    "The resource with ID '%s' is not a directory, its type is %s", id, resourceConfig.getResourceType()
                ));
            }
            return tempDirectories.computeIfAbsent(id, x -> extractFileToDisk(id, null));
        }

        @Nonnull @Override
        public File recreateAttachedDirectory(@Nonnull String id) {
            recreateIfExists(id);
            return attachedDirectory(id);
        }

        @Nonnull @Override
        public File attachedFile(@Nonnull String id) {
            Preconditions.checkHasText(id, "id cannot be null or empty");
            ResourceConfig resourceConfig = jobConfig().getResourceConfigs().get(id);
            if (resourceConfig == null) {
                throw new JetException(String.format("No resource is attached with ID '%s'", id));
            }
            if (resourceConfig.getResourceType() != FILE) {
                throw new JetException(String.format(
                    "The resource with ID '%s' is not a file, its type is %s", id, resourceConfig.getResourceType()
                ));
            }
            String fnamePath = requireNonNull(fileNameFromUrl(resourceConfig.getUrl()));
            return new File(tempDirectories.computeIfAbsent(id, x -> extractFileToDisk(id, null)), fnamePath);
        }

        @Nonnull @Override
        public File recreateAttachedFile(@Nonnull String id) {
            recreateIfExists(id);
            return attachedFile(id);
        }

        public ConcurrentHashMap<String, File> tempDirectories() {
            return tempDirectories;
        }

        private File extractFileToDisk(@Nonnull String id, @Nullable File destFile) {
            IMap<String, byte[]> map = hazelcastInstance().getMap(jobResourcesMapName(jobId()));
            try (IMapInputStream inputStream = new IMapInputStream(map, fileKeyName(id))) {
                Path destPath = (destFile == null)
                    ? Files.createTempDirectory(tempDirPrefix(hazelcastInstance().getName(), idToString(jobId()), id))
                    : destFile.toPath();
                unzip(inputStream, destPath);
                return destPath.toFile();
            } catch (IOException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        private void recreateIfExists(@Nonnull String id) {
            File dirFile = tempDirectories.get(id);
            if (dirFile == null) {
                return;
            }
            try {
                List<String> filesNotMarked =
                        editPermissionsRecursively(dirFile.toPath(), perms -> perms.add(OWNER_WRITE));
                if (!filesNotMarked.isEmpty()) {
                    logger().info("Couldn't 'chmod u+w' these files: " + filesNotMarked);
                }
                for (File file : requireNonNull(dirFile.listFiles())) {
                    IOUtil.delete(file);
                }
                extractFileToDisk(id, dirFile);
            } catch (IOException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

        @SuppressWarnings("checkstyle:magicnumber")
        private static String tempDirPrefix(String jetInstanceName, String jobId, String resourceId) {
            return "jet-" + jetInstanceName
                    + "-" + jobId
                    + "-" + resourceId.substring(0, min(32, resourceId.length())).replaceAll("[^\\w.\\-$]", "_");
        }

        @Nonnull @Override
        public ManagedContext managedContext() {
            return serializationService.getManagedContext();
        }

        @Nonnull
        public InternalSerializationService serializationService() {
            return serializationService;
        }
    }

    public static class ProcCtx extends ProcSupplierCtx implements Processor.Context {

        private final int localProcessorIndex;
        private final int globalProcessorIndex;
        private final MetricsContext metricsContext = new MetricsContext();

        @SuppressWarnings("checkstyle:ParameterNumber")
        public ProcCtx(NodeEngineImpl nodeEngine,
                       long jobId,
                       long executionId,
                       JobConfig jobConfig,
                       ILogger logger,
                       String vertexName,
                       int localProcessorIndex,
                       int globalProcessorIndex,
                       boolean isLightJob,
                       Map<Address, int[]> partitionAssignment,
                       int localParallelism,
                       int memberIndex,
                       int memberCount,
                       ConcurrentHashMap<String, File> tempDirectories,
                       InternalSerializationService serializationService,
                       Subject subject,
                       ClassLoader classLoader
        ) {
            super(nodeEngine, jobId, executionId, jobConfig, logger, vertexName, localParallelism,
                    memberCount * localParallelism, memberIndex, memberCount,
                    isLightJob, partitionAssignment, tempDirectories, serializationService, subject, classLoader);
            this.localProcessorIndex = localProcessorIndex;
            this.globalProcessorIndex = globalProcessorIndex;
        }

        @Override
        public int localProcessorIndex() {
            return localProcessorIndex;
        }

        @Override
        public int globalProcessorIndex() {
            return globalProcessorIndex;
        }

        public MetricsContext metricsContext() {
            return metricsContext;
        }
    }

    public static Container container() {
        return CONTEXT.get();
    }

    @Nonnull
    public static Processor.Context getThreadContext() {
        Container container = CONTEXT.get();
        Processor.Context context = container.getContext();
        if (context == null) {
            throw new RuntimeException("Thread %s has no context set, this method can " +
                    "be called only on threads executing the job's processors");
        }
        return context;
    }

    @Nonnull
    public static ProcCtx getCastedThreadContext() {
        Processor.Context c = getThreadContext();
        if (!(c instanceof ProcCtx)) {
            throw new RuntimeException("No real processor context - metrics not available");
        }
        return (ProcCtx) c;
    }

    public static class Container {

        @Nullable
        private Processor.Context context;

        Container() {
        }

        @Nullable
        public Processor.Context getContext() {
            return context;
        }

        public void setContext(@Nullable Processor.Context context) {
            this.context = context;
        }
    }
}
