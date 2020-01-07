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

package com.hazelcast.jet.config;

import com.hazelcast.config.MetricsConfig;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Contains the configuration specific to one Hazelcast Jet job.
 *
 * @since 3.0
 */
public class JobConfig implements IdentifiedDataSerializable {

    private static final long SNAPSHOT_INTERVAL_MILLIS_DEFAULT = SECONDS.toMillis(10);

    private String name;
    private ProcessingGuarantee processingGuarantee = ProcessingGuarantee.NONE;
    private long snapshotIntervalMillis = SNAPSHOT_INTERVAL_MILLIS_DEFAULT;
    private boolean autoScaling = true;
    private boolean splitBrainProtectionEnabled;
    private boolean enableMetrics = true;
    private boolean storeMetricsAfterJobCompletion;

    private Map<String, ResourceConfig> resourceConfigs = new LinkedHashMap<>();
    private JobClassLoaderFactory classLoaderFactory;
    private String initialSnapshotName;

    /**
     * Returns the name of the job or {@code null} if no name was given.
     */
    @Nullable
    public String getName() {
        return name;
    }

    /**
     * Sets the name of the job. There can be at most one active job in the
     * cluster with particular name, however, the name can be reused after the
     * previous job with that name completed or failed. See {@link
     * JetInstance#newJobIfAbsent}. An active job is a job that is running,
     * suspended or waiting to be run.
     * <p>
     * The job name is printed in logs and is visible in Jet Management Center.
     * <p>
     * The default value is {@code null}.
     *
     * @return {@code this} instance for fluent API
     * @since 3.0
     */
    @Nonnull
    public JobConfig setName(@Nullable String name) {
        this.name = name;
        return this;
    }

    /**
     * Tells whether {@link #setSplitBrainProtection(boolean) split brain protection}
     * is enabled.
     */
    public boolean isSplitBrainProtectionEnabled() {
        return splitBrainProtectionEnabled;
    }

    /**
     * Configures the split brain protection feature. When enabled, Jet will
     * restart the job after a topology change only if the cluster quorum is
     * satisfied. The quorum value is
     * <p>
     * {@code cluster size at job submission time / 2 + 1}.
     * <p>
     * The job can be restarted only if the size of the cluster after restart
     * is at least the quorum value. Only one of the clusters formed due to a
     * split-brain condition can satisfy the quorum. For example, if at the
     * time of job submission the cluster size was 5 and a network partition
     * causes two clusters with sizes 3 and 2 to form, the job will restart
     * only on the cluster with size 3.
     * <p>
     * Adding new nodes to the cluster after starting the job may defeat this
     * mechanism. For instance, if there are 5 members at submission time
     * (i.e., the quorum value is 3) and later a new node joins, a split into
     * two clusters of size 3 will allow the job to be restarted on both sides.
     * <p>
     * Split-brain protection is disabled by default.
     * <p>
     * If {@linkplain #setAutoScaling(boolean) auto scaling} is disabled and
     * you manually {@link Job#resume} the job, the job won't start executing
     * until the quorum is met, but will remain in the resumed state.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig setSplitBrainProtection(boolean isEnabled) {
        this.splitBrainProtectionEnabled = isEnabled;
        return this;
    }

    /**
     * Sets whether Jet will scale the job up or down when a member is added or
     * removed from the cluster. Enabled by default.
     *
     * <pre>
     * +--------------------------+-----------------------+----------------+
     * |       Auto scaling       |     Member added      | Member removed |
     * +--------------------------+-----------------------+----------------+
     * | Enabled                  | restart (after delay) | restart        |
     * | Disabled - snapshots on  | no action             | suspend        |
     * | Disabled - snapshots off | no action             | fail           |
     * +--------------------------+-----------------------+----------------+
     * </pre>
     *
     * @return {@code this} instance for fluent API
     * @see InstanceConfig#setScaleUpDelayMillis
     *        Configuring the scale-up delay
     * @see #setProcessingGuarantee
     *        Enabling/disabling snapshots
     */
    public JobConfig setAutoScaling(boolean enabled) {
        this.autoScaling = enabled;
        return this;
    }

    /**
     * Returns whether auto scaling is enabled, see {@link
     * #setAutoScaling(boolean)}.
     */
    public boolean isAutoScaling() {
        return autoScaling;
    }

    /**
     * Returns the configured {@link
     * #setProcessingGuarantee(ProcessingGuarantee) processing guarantee}.
     */
    @Nonnull
    public ProcessingGuarantee getProcessingGuarantee() {
        return processingGuarantee;
    }

    /**
     * Set the {@link ProcessingGuarantee processing guarantee} for the job.
     * When the processing guarantee is set to <i>at-least-once</i> or
     * <i>exactly-once</i>, the snapshot interval can be configured via
     * {@link #setSnapshotIntervalMillis(long)}, otherwise it will default to
     * 10 seconds.
     * <p>
     * The default value is {@link ProcessingGuarantee#NONE}.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig setProcessingGuarantee(@Nonnull ProcessingGuarantee processingGuarantee) {
        this.processingGuarantee = processingGuarantee;
        return this;
    }

    /**
     * Returns the configured {@link #setSnapshotIntervalMillis(long)
     * snapshot interval}.
     */
    public long getSnapshotIntervalMillis() {
        return snapshotIntervalMillis;
    }

    /**
     * Sets the snapshot interval in milliseconds &mdash; the interval between
     * the completion of the previous snapshot and the start of a new one. Must
     * be set to a positive value. This setting is only relevant with
     * <i>at-least-once</i> or <i>exactly-once</i> processing guarantees.
     * <p>
     * Default value is set to 10 seconds.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig setSnapshotIntervalMillis(long snapshotInterval) {
        Preconditions.checkNotNegative(snapshotInterval, "snapshotInterval can't be negative");
        this.snapshotIntervalMillis = snapshotInterval;
        return this;
    }

    /**
     * Adds the supplied classes to the list of resources that will be
     * available on the job's classpath while it's executing in the Jet
     * cluster.
     * <p>
     * See also {@link #addJar} and {@link #addClasspathResource}.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClass(@Nonnull Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            ResourceConfig cfg = new ResourceConfig(clazz);
            resourceConfigs.put(cfg.getId(), cfg);
        }
        return this;
    }

    /**
     * Adds the JAR identified by the supplied URL to the list of JARs that
     * will be a part of the job's classpath while it's executing in the Jet
     * cluster.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addJar(@Nonnull URL url) {
        return add(url, toFilename(url), ResourceType.JAR);
    }

    /**
     * Adds the supplied JAR file to the list of JARs that will be a part of
     * the job's classpath while it's executing in the Jet cluster. The JAR
     * filename will be used as the ID of the resource.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addJar(@Nonnull File file) {
        try {
            return addJar(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the JAR identified by the supplied pathname to the list of JARs
     * that will be a part of the job's classpath while it's executing in the
     * Jet cluster. The JAR filename will be used as the ID of the resource.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addJar(@Nonnull String path) {
        try {
            File file = new File(path);
            return addJar(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the ZIP file identified by the supplied URL to the list of archives
     * that will be a part of the job's classpath while it's executing in the
     * Jet cluster.
     *
     * Note: Only JAR files inside the ZIP file will be added to the
     * job classpath.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig addJarsInZip(@Nonnull URL url) {
        return add(url, toFilename(url), ResourceType.JARS_IN_ZIP);
    }

    /**
     * Adds the supplied ZIP file to the list of archives that will be a part of
     * the job's classpath while it's executing in the Jet cluster.
     *
     * Note: Only JAR files inside the ZIP file will be added to the
     * job classpath.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig addJarsInZip(@Nonnull File file) {
        try {
            return addJarsInZip(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the ZIP identified by the supplied pathname to the list of archives
     * that will be a part of the job's classpath while it's executing in the
     * Jet cluster.
     *
     * Note: Only JAR files inside the ZIP file will be added to the
     * job classpath.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig addJarsInZip(@Nonnull String path) {
        try {
            File file = new File(path);
            return addJarsInZip(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClasspathResource(@Nonnull URL url) {
        return addClasspathResource(url, toFilename(url));
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClasspathResource(@Nonnull URL url, @Nonnull String id) {
        return add(url, id, ResourceType.CLASSPATH_RESOURCE);
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource's
     * filename will be used as its ID.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClasspathResource(@Nonnull File file) {
        try {
            return addClasspathResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource
     * will be registered under the supplied ID.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClasspathResource(@Nonnull File file, @Nonnull String id) {
        try {
            return add(file.toURI().toURL(), id, ResourceType.CLASSPATH_RESOURCE);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClasspathResource(@Nonnull String path) {
        return addClasspathResource(new File(path));
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig addClasspathResource(@Nonnull String path, @Nonnull String id) {
        return addClasspathResource(new File(path), id);
    }

    /**
     * Adds the file identified by the supplied URL to the list of
     * resources that will be available to the job while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     * <p>
     * To retrieve the file from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedFile(String) ctx.attachedFile(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachFile(@Nonnull URL url) {
        return attachFile(url, toFilename(url));
    }

    /**
     * Adds the file identified by the supplied URL to the list of
     * resources that will be available to the job while it's executing in
     * the Jet cluster. The file will be registered under the supplied ID.
     * <p>
     * To retrieve the file from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedFile(String) ctx.attachedFile(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachFile(@Nonnull URL url, @Nonnull String id) {
        return add(url, id, ResourceType.FILE);
    }

    /**
     * Adds the supplied file to the list of resources that will be available to
     * the job while it's executing in the Jet cluster. The filename will be
     * used as its ID.
     * <p>
     * To retrieve the file from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedFile(String) ctx.attachedFile(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachFile(@Nonnull File file) {
        try {
            return attachFile(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the supplied file to the list of files that will be available to
     * the job while it's executing in the Jet cluster.
     * The file will be registered under the supplied ID.
     * <p>
     * To retrieve the file from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedFile(String) ctx.attachedFile(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachFile(@Nonnull File file, @Nonnull String id) {
        try {
            return add(file.toURI().toURL(), id, ResourceType.FILE);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the file identified by the supplied pathname to the list of
     * files that will be available to the job while it's executing in
     * the Jet cluster. The filename will be used as its ID.
     * <p>
     * To retrieve the file from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedFile(String) ctx.attachedFile(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachFile(@Nonnull String path) {
        return attachFile(new File(path));
    }

    /**
     * Adds the file identified by the supplied pathname to the list of
     * files that will be available to the job while it's executing in
     * the Jet cluster. The file will be registered under the supplied ID.
     * <p>
     * To retrieve the file from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedFile(String) ctx.attachedFile(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachFile(@Nonnull String path, @Nonnull String id) {
        return attachFile(new File(path), id);
    }

    /**
     * Adds the directory identified by the supplied pathname to the list of
     * files that will be available to the job while it's executing in the Jet
     * cluster. The directory name will be used as its ID. Hidden files in the
     * directory will be ignored.
     * <p>
     * To retrieve the directory from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedDirectory(String) ctx.attachedDirectory(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachDirectory(@Nonnull String path) {
        return attachDirectory(new File(path));
    }

    /**
     * Adds the directory identified by the supplied pathname to the list of
     * files that will be available to the job while it's executing in the Jet
     * cluster. The directory will be registered under the supplied ID. Hidden
     * files in the directory will be ignored.
     * <p>
     * To retrieve the directory from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedDirectory(String) ctx.attachedDirectory(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachDirectory(@Nonnull String path, @Nonnull String id) {
        return attachDirectory(new File(path), id);
    }

    /**
     * Adds the supplied directory to the list of files that will be available
     * to the job while it's executing in the Jet cluster. The directory name
     * will be used as its ID. Hidden files in the directory will be ignored.
     * <p>
     * To retrieve the directory from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedDirectory(String) ctx.attachedDirectory(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachDirectory(@Nonnull File file) {
        return attachDirectory(file, file.getName());
    }

    /**
     * Adds the supplied directory to the list of files that will be available
     * to the job while it's executing in the Jet cluster. The directory will
     * be registered under the supplied ID. Hidden files in the directory will
     * be ignored.
     * <p>
     * To retrieve the directory from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedDirectory(String) ctx.attachedDirectory(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachDirectory(@Nonnull File file, @Nonnull String id) {
        try {
            return add(file.toURI().toURL(), id, ResourceType.DIRECTORY);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the directory identified by the supplied URL to the list of files
     * that will be available to the job while it's executing in the Jet
     * cluster. The directory name will be used as its ID. Hidden files in the
     * directory will be ignored.
     * <p>
     * To retrieve the directory from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedDirectory(String) ctx.attachedDirectory(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachDirectory(@Nonnull URL url) {
        return attachDirectory(url, toFilename(url));
    }

    /**
     * Adds the directory identified by the supplied URL to the list of files
     * that will be available to the job while it's executing in the Jet
     * cluster. The directory will be registered under the supplied ID. Hidden
     * files in the directory will be ignored.
     * <p>
     * To retrieve the directory from within the Jet job, call {@link
     * ProcessorSupplier.Context#attachedDirectory(String) ctx.attachedDirectory(id)},
     * where {@code ctx} is the {@code ProcessorSupplier} context available,
     * for example, to {@link ServiceFactory#createContextFn()}.
     *
     * @return {@code this} instance for fluent API
     * @since 4.0
     */
    @Nonnull
    public JobConfig attachDirectory(@Nonnull URL url, @Nonnull String id) {
        return add(url, id, ResourceType.DIRECTORY);
    }

    /**
     * Returns all the registered resource configurations.
     */
    @Nonnull
    public Map<String, ResourceConfig> getResourceConfigs() {
        return resourceConfigs;
    }

    private JobConfig add(@Nonnull URL url, @Nonnull String id, @Nonnull ResourceType resourceType) {
        ResourceConfig cfg = new ResourceConfig(url, id, resourceType);
        if (resourceConfigs.putIfAbsent(id, cfg) != null) {
            throw new IllegalArgumentException("Resource with id:" + id + " already exists");
        }
        return this;
    }

    private static String toFilename(URL url) {
        String urlFile = url.getPath();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1);
    }

    /**
     * Sets a custom {@link JobClassLoaderFactory} that will be used to load
     * job classes and resources on Jet members.
     *
     * @return {@code this} instance for fluent API
     */
    @Nonnull
    public JobConfig setClassLoaderFactory(@Nullable JobClassLoaderFactory classLoaderFactory) {
        this.classLoaderFactory = classLoaderFactory;
        return this;
    }

    /**
     * Returns the configured {@link JobClassLoaderFactory}.
     */
    @Nullable
    public JobClassLoaderFactory getClassLoaderFactory() {
        return classLoaderFactory;
    }

    /**
     * Returns the configured {@linkplain #setInitialSnapshotName(String)
     * initial snapshot name} or {@code null} if no initial snapshot is configured.
     */
    @Nullable
    public String getInitialSnapshotName() {
        return initialSnapshotName;
    }

    /**
     * Sets the {@linkplain Job#exportSnapshot(String) exported state snapshot}
     * name to restore the initial job state from. This state will be used for
     * initial state and also for the case when the execution restarts before
     * it produces first snapshot.
     * <p>
     * The job will use the state even if {@linkplain
     * #setProcessingGuarantee(ProcessingGuarantee) processing guarantee} is
     * set to {@link ProcessingGuarantee#NONE}.
     *
     * @param initialSnapshotName the snapshot name given to {@link
     *       Job#exportSnapshot(String)}
     * @return {@code this} instance for fluent API
     * @since 3.0
     */
    @Nonnull
    public JobConfig setInitialSnapshotName(@Nullable String initialSnapshotName) {
        this.initialSnapshotName = initialSnapshotName;
        return this;
    }

    /**
     * Sets whether metrics collection should be enabled for the job. Needs
     * {@link MetricsConfig#isEnabled()} to be on in order to function.
     * <p>
     * Metrics for running jobs can be queried using {@link Job#getMetrics()}
     * It's enabled by default.
     *
     * @since 3.2
     */
    @Nonnull
    public JobConfig setMetricsEnabled(boolean enabled) {
        this.enableMetrics = enabled;
        return this;
    }

    /**
     * Returns if metrics collection is enabled for the job.
     *
     * @since 3.2
     */
    public boolean isMetricsEnabled() {
        return enableMetrics;
    }

    /**
     * Returns whether metrics should be stored in the cluster after the job
     * completes. Needs both {@link MetricsConfig#isEnabled()} and
     * {@link #isMetricsEnabled()} to be on in order to function.
     * <p>
     * If enabled, metrics can be retrieved by calling
     * {@link Job#getMetrics()}.
     * <p>
     * It's disabled by default.
     *
     * @since 3.2
     */
    public boolean isStoreMetricsAfterJobCompletion() {
        return storeMetricsAfterJobCompletion;
    }

    /**
     * Sets whether metrics should be stored in the cluster after the job
     * completes. If enabled, metrics can be retrieved for the configured
     * job even if it's no longer running (has completed successfully, has
     * failed, has been cancelled or suspended) by calling {@link Job#getMetrics()}.
     * <p>
     * If disabled, once the configured job stops running {@link Job#getMetrics()}
     * will always return empty metrics for it, regardless of the settings
     * for {@link MetricsConfig#setEnabled global metrics collection}
     * or {@link JobConfig#isMetricsEnabled() per job metrics collection}.
     * <p>
     * It's disabled by default.
     *
     * @since 3.2
     */
    public JobConfig setStoreMetricsAfterJobCompletion(boolean storeMetricsAfterJobCompletion) {
        this.storeMetricsAfterJobCompletion = storeMetricsAfterJobCompletion;
        return this;
    }

    @Override
    public int getFactoryId() {
        return JetConfigDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetConfigDataSerializerHook.JOB_CONFIG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeObject(processingGuarantee);
        out.writeLong(snapshotIntervalMillis);
        out.writeBoolean(autoScaling);
        out.writeBoolean(splitBrainProtectionEnabled);
        out.writeObject(resourceConfigs);
        out.writeObject(classLoaderFactory);
        out.writeUTF(initialSnapshotName);
        out.writeBoolean(enableMetrics);
        out.writeBoolean(storeMetricsAfterJobCompletion);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        processingGuarantee = in.readObject();
        snapshotIntervalMillis = in.readLong();
        autoScaling = in.readBoolean();
        splitBrainProtectionEnabled = in.readBoolean();
        resourceConfigs = in.readObject();
        classLoaderFactory = in.readObject();
        initialSnapshotName = in.readUTF();
        enableMetrics = in.readBoolean();
        storeMetricsAfterJobCompletion = in.readBoolean();
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobConfig jobConfig = (JobConfig) o;
        return snapshotIntervalMillis == jobConfig.snapshotIntervalMillis &&
                autoScaling == jobConfig.autoScaling &&
                splitBrainProtectionEnabled == jobConfig.splitBrainProtectionEnabled &&
                enableMetrics == jobConfig.enableMetrics &&
                storeMetricsAfterJobCompletion == jobConfig.storeMetricsAfterJobCompletion &&
                Objects.equals(name, jobConfig.name) &&
                processingGuarantee == jobConfig.processingGuarantee &&
                Objects.equals(resourceConfigs, jobConfig.resourceConfigs) &&
                Objects.equals(classLoaderFactory, jobConfig.classLoaderFactory) &&
                Objects.equals(initialSnapshotName, jobConfig.initialSnapshotName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, processingGuarantee, snapshotIntervalMillis, autoScaling,
                splitBrainProtectionEnabled, enableMetrics, storeMetricsAfterJobCompletion, resourceConfigs,
                classLoaderFactory, initialSnapshotName
        );
    }
}
