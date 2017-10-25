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

package com.hazelcast.jet.config;

import com.hazelcast.util.Preconditions;

import java.io.File;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Contains the configuration specific to one Hazelcast Jet job.
 */
public class JobConfig implements Serializable {

    private static final int SNAPSHOT_INTERVAL_MILLIS_DEFAULT = 10_000;

    private ProcessingGuarantee processingGuarantee = ProcessingGuarantee.NONE;
    private long snapshotIntervalMillis = SNAPSHOT_INTERVAL_MILLIS_DEFAULT;

    private boolean splitBrainProtectionEnabled;
    private final List<ResourceConfig> resourceConfigs = new ArrayList<>();
    private boolean autoRestartEnabled = true;

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
     * This setting has no effect if
     * {@link #setAutoRestartOnMemberFailure(boolean) auto restart on member
     * failure} is disabled.
     */
    public JobConfig setSplitBrainProtection(boolean isEnabled) {
        this.splitBrainProtectionEnabled = isEnabled;
        return this;
    }

    /**
     * Tells whether {@link #setAutoRestartOnMemberFailure(boolean) auto
     * restart after member failure} is enabled.
     */
    public boolean isAutoRestartOnMemberFailureEnabled() {
        return this.autoRestartEnabled;
    }

    /**
     * Sets whether the job should automatically restart after a
     * participating member leaves the cluster. When enabled and a member
     * fails, the job will automatically restart on the remaining members.
     * <p>
     * If snapshotting is enabled, the job state will be restored from the
     * latest snapshot.
     * <p>
     * By default, auto-restart is enabled.
     */
    public JobConfig setAutoRestartOnMemberFailure(boolean isEnabled) {
        this.autoRestartEnabled = isEnabled;
        return this;
    }

    /**
     * Returns the configured {@link
     * #setProcessingGuarantee(ProcessingGuarantee) processing guarantee}.
     */
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
     */
    public JobConfig setProcessingGuarantee(ProcessingGuarantee processingGuarantee) {
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
     * the completion of the previous snapshot and the start of a new one.
     * Must be set to a positive value. This setting is only relevant when
     * <i>>at-least-once</i> or <i>exactly-once</i> processing guarantees are used.
     * <p>
     * Default value is set to 10 seconds.
     */
    public JobConfig setSnapshotIntervalMillis(long snapshotInterval) {
        Preconditions.checkPositive(snapshotInterval, "snapshotInterval must be positive");
        this.snapshotIntervalMillis = snapshotInterval;
        return this;
    }

    /**
     * Adds the supplied classes to the list of resources that will be
     * available on the job's classpath while it's executing in the Jet
     * cluster.
     */
    public JobConfig addClass(Class... classes) {
        checkNotNull(classes, "Classes can not be null");

        for (Class clazz : classes) {
            resourceConfigs.add(new ResourceConfig(clazz));
        }
        return this;
    }

    /**
     * Adds the JAR identified by the supplied URL to the list of JARs that
     * will be a part of the job's classpath while it's executing in the Jet
     * cluster.
     */
    public JobConfig addJar(URL url) {
        return add(url, null, true);
    }

    /**
     * Adds the supplied JAR file to the list of JARs that will be a part of
     * the job's classpath while it's executing in the Jet cluster. The JAR
     * filename will be used as the ID of the resource.
     */
    public JobConfig addJar(File file) {
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
     */
    public JobConfig addJar(String path) {
        try {
            File file = new File(path);
            return addJar(file.toURI().toURL());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     */
    public JobConfig addResource(URL url) {
        return addResource(url, toFilename(url));
    }

    /**
     * Adds the resource identified by the supplied URL to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     */
    public JobConfig addResource(URL url, String id) {
        return add(url, id, false);
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource's
     * filename will be used as its ID.
     */
    public JobConfig addResource(File file) {
        try {
            return addResource(file.toURI().toURL(), file.getName());
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the supplied file to the list of resources that will be on the
     * job's classpath while it's executing in the Jet cluster. The resource
     * will be registered under the supplied ID.
     */
    public JobConfig addResource(File file, String id) {
        try {
            return add(file.toURI().toURL(), id, false);
        } catch (MalformedURLException e) {
            throw rethrow(e);
        }
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource's filename will be used as its ID.
     */
    public JobConfig addResource(String path) {
        return addResource(new File(path));
    }

    /**
     * Adds the resource identified by the supplied pathname to the list of
     * resources that will be on the job's classpath while it's executing in
     * the Jet cluster. The resource will be registered under the supplied ID.
     */
    public JobConfig addResource(String path, String id) {
        return addResource(new File(path), id);
    }

    /**
     * Returns all the registered resource configurations.
     */
    public List<ResourceConfig> getResourceConfigs() {
        return resourceConfigs;
    }

    private JobConfig add(URL url, String id, boolean isJar) {
        resourceConfigs.add(new ResourceConfig(url, id, isJar));
        return this;
    }

    private static String toFilename(URL url) {
        String urlFile = url.getPath();
        return urlFile.substring(urlFile.lastIndexOf('/') + 1, urlFile.length());
    }

}
