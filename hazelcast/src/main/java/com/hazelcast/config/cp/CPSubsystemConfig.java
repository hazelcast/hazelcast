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

package com.hazelcast.config.cp;

import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.matcher.MatchingPointConfigPatternMatcher;
import com.hazelcast.cp.ISemaphore;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.cp.CPGroup;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.CPSubsystem;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.cp.session.CPSession;
import com.hazelcast.cp.session.CPSessionManagementService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.config.ConfigUtils.lookupByPattern;
import static com.hazelcast.partition.strategy.StringPartitioningStrategy.getBaseName;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkPositive;
import static com.hazelcast.internal.util.Preconditions.checkTrue;

/**
 * Contains configuration options for CP Subsystem.
 * <p>
 * You can check the following code snippet to see how CP Subsystem
 * can be initialized by configuring only the
 * {@link CPSubsystemConfig#setCPMemberCount(int)} value. In this code,
 * we set 3 to {@link CPSubsystemConfig#setCPMemberCount(int)}, and we don't
 * set any value to {@link CPSubsystemConfig#setGroupSize(int)}. Therefore,
 * there will be 3 CP members in CP Subsystem and each CP groups will have
 * 3 CP members as well.
 * <pre>
 *     int cpMemberCount = 3;
 *     int apMemberCount = 2;
 *     int memberCount = cpMemberCount + apMemberCount;
 *     Config config = new Config();
 *     config.getCPSubsystemConfig().setCPMemberCount(cpMemberCount);
 *     HazelcastInstance[] instances = new HazelcastInstance[memberCount];
 *     for (int i = 0; i &lt; memberCount; i++) {
 *         instances[i] = Hazelcast.newHazelcastInstance(config);
 *     }
 *
 *     // update an atomic long via a CP member
 *     IAtomicLong cpLong = instances[0].getCPSubsystem().getAtomicLong("myLong");
 *     cpLong.set(10);
 *
 *     // access to its value via an AP member
 *     cpLong = instances[cpMemberCount].getCPSubsystem().getAtomicLong("myLong");
 *     System.out.println(cpLong.get());
 * </pre>
 * <p>
 * In the following code snippet, we configure
 * {@link CPSubsystemConfig#setCPMemberCount(int)} to 5 and
 * {@link CPSubsystemConfig#setGroupSize(int)} to 3, therefore there will be 5
 * CP members and CP groups will be initialized by selecting 3 random CP members
 * among them.
 * <pre>
 *     int cpMemberCount = 5;
 *     int apMemberCount = 2;
 *     int groupSize = 3;
 *     int memberCount = cpMemberCount + apMemberCount;
 *     Config config = new Config();
 *     config.getCPSubsystemConfig()
 *           .setCPMemberCount(cpMemberCount)
 *           .setGroupSize(groupSize);
 *     HazelcastInstance[] instances = new HazelcastInstance[memberCount];
 *     for (int i = 0; i &lt; memberCount; i++) {
 *         instances[i] = Hazelcast.newHazelcastInstance(config);
 *     }
 *
 *     // update an atomic long via a CP member
 *     IAtomicLong cpLong = instances[0].getCPSubsystem().getAtomicLong("myLong");
 *     cpLong.set(10);
 *
 *     // access to its value via an AP member
 *     cpLong = instances[cpMemberCount].getCPSubsystem().getAtomicLong("myLong");
 *     System.out.println(cpLong.get());
 * </pre>
 *
 * @see CPSubsystem
 * @see CPMember
 * @see CPSession
 */
@SuppressWarnings("checkstyle:MethodCount")
public class CPSubsystemConfig {

    /**
     * The default value for a CP session to be kept alive after the last
     * heartbeat it has received. See {@link #sessionTimeToLiveSeconds}
     */
    public static final int DEFAULT_SESSION_TTL_SECONDS = (int) TimeUnit.MINUTES.toSeconds(5);

    /**
     * The default duration for the periodically-committed CP session
     * heartbeats. See {@link #sessionHeartbeatIntervalSeconds}
     */
    public static final int DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;

    /**
     * The minimum number of CP members that can form a CP group.
     * See {@link #groupSize}
     */
    public static final int MIN_GROUP_SIZE = 3;

    /**
     * The maximum number of CP members that can form a CP group.
     * Theoretically, there is no upper bound on the number of CP members to
     * run the Raft consensus algorithm. However, since the Raft consensus
     * algorithm synchronously replicates operations to the majority of a CP
     * group members, a larger CP group means more replication overhead, and
     * memory consumption as well. The current maximum CP group size limit
     * offers a sufficient degree of fault tolerance for CP Subsystem usages.
     * <p>
     * See {@link #groupSize}
     */
    public static final int MAX_GROUP_SIZE = 7;

    /**
     * The default duration to wait before automatically removing
     * a missing CP member from CP Subsystem.
     * See {@link #missingCPMemberAutoRemovalSeconds}
     */
    public static final int DEFAULT_MISSING_CP_MEMBER_AUTO_REMOVAL_SECONDS = (int) TimeUnit.HOURS.toSeconds(4);


    /**
     * The default directory name for storing CP data.
     * See {@link #baseDir}
     */
    public static final String CP_BASE_DIR_DEFAULT = "cp-data";

    /**
     * The default data load timeout duration for restoring CP data from disk.
     * See {@link #dataLoadTimeoutSeconds}
     */
    public static final int DEFAULT_DATA_LOAD_TIMEOUT_SECONDS = 2 * 60;

    /**
     * Number of CP members to initialize CP Subsystem. It is 0 by default,
     * meaning that CP Subsystem is disabled. CP Subsystem is enabled when
     * a positive value is set. After CP Subsystem is initialized successfully,
     * more CP members can be added at run-time and the number of active CP
     * members can go beyond the configured CP member count. The number of CP
     * members can be smaller than total member count of the Hazelcast cluster.
     * For instance, you can run 5 CP members in a Hazelcast cluster of
     * 20 members.
     * <p>
     * If set, must be greater than or equal to {@link #groupSize}
     */
    private int cpMemberCount;

    /**
     * Number of CP members to form CP groups. If set, it must be an odd
     * number between {@link #MIN_GROUP_SIZE} and {@link #MAX_GROUP_SIZE}.
     * Otherwise, {@link #cpMemberCount} is respected while forming CP groups.
     * <p>
     * If set, must be smaller than or equal to {@link #cpMemberCount}
     */
    private int groupSize;

    /**
     * Duration for a CP session to be kept alive after the last received
     * session heartbeat. A CP session is closed if no session heartbeat is
     * received during this duration. Session TTL must be decided wisely. If
     * a very small value is set, a CP session can be closed prematurely if
     * its owner Hazelcast instance temporarily loses connectivity to CP
     * Subsystem because of a network partition or a GC pause. In such an
     * occasion, all CP resources of this Hazelcast instance, such as
     * {@link FencedLock} or {@link ISemaphore}, are released. On the other
     * hand, if a very large value is set, CP resources can remain assigned to
     * an actually crashed Hazelcast instance for too long and liveliness
     * problems can occur. CP Subsystem offers an API in
     * {@link CPSessionManagementService} to deal with liveliness issues
     * related to CP sessions. In order to prevent premature session expires,
     * session TTL configuration can be set a relatively large value and
     * {@link CPSessionManagementService#forceCloseSession(String, long)}
     * can be manually called to close CP session of a crashed Hazelcast
     * instance.
     * <p>
     * Must be greater than {@link #sessionHeartbeatIntervalSeconds}, and
     * smaller than or equal to {@link #missingCPMemberAutoRemovalSeconds}
     */
    private int sessionTimeToLiveSeconds = DEFAULT_SESSION_TTL_SECONDS;

    /**
     * Interval for the periodically-committed CP session heartbeats.
     * A CP session is started on a CP group with the first session-based
     * request of a Hazelcast instance. After that moment, heartbeats are
     * periodically committed to the CP group.
     * <p>
     * Must be smaller than {@link #sessionTimeToLiveSeconds}
     */
    private int sessionHeartbeatIntervalSeconds = DEFAULT_HEARTBEAT_INTERVAL_SECONDS;

    /**
     * Duration to wait before automatically removing a missing CP member from
     * CP Subsystem. When a CP member leaves the Hazelcast cluster, it is not
     * automatically removed from CP Subsystem, since it could be still alive
     * and left the cluster because of a network problem. On the other hand,
     * if a missing CP member actually crashed, it creates a danger for CP
     * groups, because it is still part of majority calculations. This
     * situation could lead to losing majority of CP groups if multiple CP
     * members leave the cluster over time.
     * <p>
     * With the default configuration, missing CP members are automatically
     * removed from CP Subsystem after 4 hours. This feature is very useful
     * in terms of fault tolerance when CP member count is also configured
     * to be larger than group size. In this case, a missing CP member is
     * safely replaced in its CP groups with other available CP members
     * in CP Subsystem. This configuration also implies that no network
     * partition is expected to be longer than the configured duration.
     * <p>
     * If a missing CP member comes back alive after it is removed from CP
     * Subsystem with this feature, that CP member must be terminated manually.
     * <p>
     * Must be greater than or equal to {@link #sessionTimeToLiveSeconds}
     */
    private int missingCPMemberAutoRemovalSeconds = DEFAULT_MISSING_CP_MEMBER_AUTO_REMOVAL_SECONDS;

    /**
     * Offers a choice between at-least-once and at-most-once execution
     * of operations on top of the Raft consensus algorithm. It is disabled by
     * default and offers at-least-once execution guarantee. If enabled, it
     * switches to at-most-once execution guarantee. When you invoke an API
     * method on a CP data structure proxy, it sends an internal operation
     * to the corresponding CP group. After this operation is committed on
     * the majority of this CP group by the Raft leader node, it sends
     * a response for the public API call. If a failure causes loss of
     * the response, then the calling side cannot determine if the operation is
     * committed on the CP group or not. In this case, if this configuration is
     * disabled, the operation is replicated again to the CP group, and hence
     * could be committed multiple times. If it is enabled, the public API call
     * fails with {@link IndeterminateOperationStateException}.
     */
    private boolean failOnIndeterminateOperationState;

    /**
     * Flag to denote whether or not CP Subsystem Persistence is enabled.
     * If enabled, CP members persist their local CP data to stable storage and
     * can recover from crashes.
     */
    private boolean persistenceEnabled;

    /**
     * Base directory to store all CP data when {@link #persistenceEnabled}
     * is true. This directory can be shared between multiple CP members.
     * Each CP member creates a unique directory for itself under the base
     * directory. This is especially useful for cloud environments where CP
     * members generally use a shared filesystem.
     */
    private File baseDir = new File(CP_BASE_DIR_DEFAULT);

    /**
     * Timeout duration for CP members to restore their data from disk.
     * A CP member fails its startup if it cannot complete its CP data restore
     * process in the configured duration.
     */
    private int dataLoadTimeoutSeconds = DEFAULT_DATA_LOAD_TIMEOUT_SECONDS;

    /**
     * Contains configuration options for Hazelcast's Raft consensus algorithm
     * implementation.
     */
    private RaftAlgorithmConfig raftAlgorithmConfig = new RaftAlgorithmConfig();

    /**
     * Configurations for CP {@link ISemaphore} instances.
     */
    private final Map<String, SemaphoreConfig> semaphoreConfigs = new ConcurrentHashMap<String, SemaphoreConfig>();

    /**
     * Configurations for {@link FencedLock} instances.
     */
    private final Map<String, FencedLockConfig> lockConfigs = new ConcurrentHashMap<String, FencedLockConfig>();

    private final ConfigPatternMatcher configPatternMatcher = new MatchingPointConfigPatternMatcher();

    public CPSubsystemConfig() {
    }

    public CPSubsystemConfig(CPSubsystemConfig config) {
        this.cpMemberCount = config.cpMemberCount;
        this.groupSize = config.groupSize;
        this.raftAlgorithmConfig = new RaftAlgorithmConfig(config.raftAlgorithmConfig);
        this.sessionTimeToLiveSeconds = config.sessionTimeToLiveSeconds;
        this.sessionHeartbeatIntervalSeconds = config.sessionHeartbeatIntervalSeconds;
        this.failOnIndeterminateOperationState = config.failOnIndeterminateOperationState;
        this.missingCPMemberAutoRemovalSeconds = config.missingCPMemberAutoRemovalSeconds;
        this.persistenceEnabled = config.persistenceEnabled;
        this.baseDir = config.baseDir;
        this.dataLoadTimeoutSeconds = config.dataLoadTimeoutSeconds;
        for (SemaphoreConfig semaphoreConfig : config.semaphoreConfigs.values()) {
            this.semaphoreConfigs.put(semaphoreConfig.getName(), new SemaphoreConfig(semaphoreConfig));
        }
        for (FencedLockConfig lockConfig : config.lockConfigs.values()) {
            this.lockConfigs.put(lockConfig.getName(), new FencedLockConfig(lockConfig));
        }
    }

    /**
     * Returns the number of CP members that will initialize CP Subsystem.
     * CP Subsystem is disabled if it is 0.
     *
     * @return the number of CP members that will initialize CP Subsystem
     */
    public int getCPMemberCount() {
        return cpMemberCount;
    }

    /**
     * Sets the CP member count to initialize CP Subsystem. CP Subsystem is
     * disabled if 0. Cannot be smaller than {@link #MIN_GROUP_SIZE} and
     * {@link #groupSize}
     *
     * @return this config instance
     */
    public CPSubsystemConfig setCPMemberCount(int cpMemberCount) {
        checkTrue(cpMemberCount == 0 || cpMemberCount >= MIN_GROUP_SIZE, "CP Subsystem must have at least "
                + MIN_GROUP_SIZE + " CP members");
        this.cpMemberCount = cpMemberCount;
        return this;
    }

    /**
     * Returns the number of CP members to form CP groups.
     * Returns 0 if {@link #cpMemberCount} is 0.
     * If group size is not set:
     * - returns the CP member count if it is an odd number,
     * - returns the CP member count - 1 if it is an even number.
     *
     * @return the number of CP members to form CP groups
     */
    public int getGroupSize() {
        if (groupSize > 0 || cpMemberCount == 0) {
            return groupSize;
        }

        int groupSize = cpMemberCount;
        if (groupSize % 2 == 0) {
            groupSize--;
        }

        return Math.min(groupSize, MAX_GROUP_SIZE);
    }

    /**
     * Sets the number of CP members to form CP groups.
     * Must be an odd number between {@link #MIN_GROUP_SIZE}
     * and {@link #MAX_GROUP_SIZE}.
     *
     * @return this config instance
     */
    @SuppressFBWarnings(value = "IM_BAD_CHECK_FOR_ODD", justification = "It's obvious that groupSize is not negative.")
    public CPSubsystemConfig setGroupSize(int groupSize) {
        checkTrue(groupSize == 0 || (groupSize >= MIN_GROUP_SIZE && groupSize <= MAX_GROUP_SIZE
                && (groupSize % 2 == 1)), "Group size must be an odd value between 3 and 7");
        this.groupSize = groupSize;
        return this;
    }

    /**
     * Returns the duration for a CP session to be kept alive
     * after its last session heartbeat.
     *
     * @return the duration for a CP session to be kept alive
     * after its last session heartbeat
     */
    public int getSessionTimeToLiveSeconds() {
        return sessionTimeToLiveSeconds;
    }

    /**
     * Sets the duration for a CP session to be kept alive
     * after its last session heartbeat.
     *
     * @return this config instance
     */
    public CPSubsystemConfig setSessionTimeToLiveSeconds(int sessionTimeToLiveSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session TTL must be a positive value!");
        this.sessionTimeToLiveSeconds = sessionTimeToLiveSeconds;
        return this;
    }

    /**
     * Returns the interval for the periodically-committed CP session
     * heartbeats.
     *
     * @return the interval for the periodically-committed CP session
     * heartbeats
     */
    public int getSessionHeartbeatIntervalSeconds() {
        return sessionHeartbeatIntervalSeconds;
    }

    /**
     * Sets the interval for the periodically-committed CP session heartbeats.
     *
     * @return this config instance
     */
    public CPSubsystemConfig setSessionHeartbeatIntervalSeconds(int sessionHeartbeatIntervalSeconds) {
        checkPositive(sessionTimeToLiveSeconds, "Session heartbeat interval must be a positive value!");
        this.sessionHeartbeatIntervalSeconds = sessionHeartbeatIntervalSeconds;
        return this;
    }

    /**
     * Returns the duration to wait before automatically removing a missing
     * CP member from CP Subsystem
     *
     * @return the duration to wait before automatically removing a missing
     * CP member from the CP Subsystem
     */
    public int getMissingCPMemberAutoRemovalSeconds() {
        return missingCPMemberAutoRemovalSeconds;
    }

    /**
     * Sets the duration to wait before automatically removing a missing
     * CP member from CP Subsystem.
     *
     * @return this config instance
     */
    public CPSubsystemConfig setMissingCPMemberAutoRemovalSeconds(int missingCPMemberAutoRemovalSeconds) {
        checkTrue(missingCPMemberAutoRemovalSeconds >= 0, "missing cp member auto-removal seconds must be non-negative");
        this.missingCPMemberAutoRemovalSeconds = missingCPMemberAutoRemovalSeconds;
        return this;
    }

    /**
     * Returns the value to determine if CP Subsystem API calls will fail when
     * result of an API call becomes indeterminate.
     *
     * @return the value to determine if CP Subsystem calls will fail when
     * result of an API call becomes indeterminate
     */
    public boolean isFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    /**
     * Sets the value to determine if CP Subsystem calls will fail when
     * result of an API call becomes indeterminate.
     *
     * @return this config instance
     */
    public CPSubsystemConfig setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    /**
     * Returns whether CP Subsystem Persistence enabled on this member.
     *
     * @return true if CP Subsystem Persistence is enabled, false otherwise
     */
    public boolean isPersistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Sets whether CP Subsystem Persistence is enabled on this member.
     *
     * @return this config instance
     */
    public CPSubsystemConfig setPersistenceEnabled(boolean persistenceEnabled) {
        this.persistenceEnabled = persistenceEnabled;
        return this;
    }

    /**
     * Returns the base directory for persisting CP data.
     * Can be an absolute or relative path to the node startup directory.
     *
     * @return returns the base directory for CP data
     */
    public File getBaseDir() {
        return baseDir;
    }

    /**
     * Sets the base directory for persisting CP data.
     * Can be an absolute or relative path to the node startup directory.
     *
     * @param baseDir base directory
     * @return this config instance
     */
    public CPSubsystemConfig setBaseDir(File baseDir) {
        checkNotNull(baseDir);
        this.baseDir = baseDir;
        return this;
    }

    /**
     * Returns the timeout duration for CP members to restore their data from
     * stable storage. A CP member fails its startup if it cannot complete its
     * CP data restore process before this timeout duration.
     *
     * @return the timeout duration for CP members to restore their data from
     * stable storage
     */
    public int getDataLoadTimeoutSeconds() {
        return dataLoadTimeoutSeconds;
    }

    /**
     * Sets the timeout duration for CP members to restore their data from
     * stable storage. A CP member fails its startup if it cannot complete its
     * CP data restore process before this timeout duration.
     *
     * @param dataLoadTimeoutSeconds the timeout duration for CP members to
     *                               restore their data from stable storage
     * @return this config instance
     */
    public CPSubsystemConfig setDataLoadTimeoutSeconds(int dataLoadTimeoutSeconds) {
        checkPositive(dataLoadTimeoutSeconds, "data load timeout seconds must be positive");
        this.dataLoadTimeoutSeconds = dataLoadTimeoutSeconds;
        return this;
    }

    /**
     * Returns configuration options for Hazelcast's Raft consensus algorithm
     * implementation
     *
     * @return configuration options for Hazelcast's Raft consensus algorithm
     * implementation
     */
    public RaftAlgorithmConfig getRaftAlgorithmConfig() {
        return raftAlgorithmConfig;
    }

    /**
     * Sets configuration options for Hazelcast's Raft consensus algorithm
     * implementation
     *
     * @return this config instance
     */
    public CPSubsystemConfig setRaftAlgorithmConfig(RaftAlgorithmConfig raftAlgorithmConfig) {
        checkNotNull(raftAlgorithmConfig);
        this.raftAlgorithmConfig = raftAlgorithmConfig;
        return this;
    }

    /**
     * Returns the map of CP {@link ISemaphore} configurations
     *
     * @return the map of CP {@link ISemaphore} configurations
     */
    public Map<String, SemaphoreConfig> getSemaphoreConfigs() {
        return semaphoreConfigs;
    }

    /**
     * Returns the CP {@link ISemaphore} configuration for the given name.
     * <p>
     * The name is matched by stripping the {@link CPGroup} name from
     * the given {@code name} if present.
     * Returns null if there is no config found by the given {@code name}
     *
     * @param name name of the CP {@link ISemaphore}
     * @return the CP {@link ISemaphore} configuration
     */
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return lookupByPattern(configPatternMatcher, semaphoreConfigs, getBaseName(name));
    }

    /**
     * Adds the CP {@link ISemaphore} configuration. Name of the CP
     * {@link ISemaphore} could optionally contain a {@link CPGroup} name,
     * like "mySemaphore@group1".
     *
     * @param semaphoreConfig the CP {@link ISemaphore} configuration
     * @return this config instance
     */
    public CPSubsystemConfig addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        semaphoreConfigs.put(semaphoreConfig.getName(), semaphoreConfig);
        return this;
    }

    /**
     * Sets the map of CP {@link ISemaphore} configurations,
     * mapped by config name. Names could optionally contain
     * a {@link CPGroup} name, such as "mySemaphore@group1".
     *
     * @param semaphoreConfigs the CP {@link ISemaphore} config map to set
     * @return this config instance
     */
    public CPSubsystemConfig setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        this.semaphoreConfigs.clear();
        this.semaphoreConfigs.putAll(semaphoreConfigs);
        for (Entry<String, SemaphoreConfig> entry : this.semaphoreConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    /**
     * Returns the map of {@link FencedLock} configurations
     *
     * @return the map of {@link FencedLock} configurations
     */
    public Map<String, FencedLockConfig> getLockConfigs() {
        return lockConfigs;
    }

    /**
     * Returns the {@link FencedLock} configuration for the given name.
     * <p>
     * The name is matched by stripping the {@link CPGroup} name from
     * the given {@code name} if present.
     * Returns null if there is no config found by the given {@code name}
     *
     * @param name name of the {@link FencedLock}
     * @return the {@link FencedLock} configuration
     */
    public FencedLockConfig findLockConfig(String name) {
        return lookupByPattern(configPatternMatcher, lockConfigs, getBaseName(name));
    }

    /**
     * Adds the {@link FencedLock} configuration. Name of the
     * {@link FencedLock} could optionally contain a {@link CPGroup} name,
     * like "myLock@group1".
     *
     * @param lockConfig the {@link FencedLock} configuration
     * @return this config instance
     */
    public CPSubsystemConfig addLockConfig(FencedLockConfig lockConfig) {
        lockConfigs.put(lockConfig.getName(), lockConfig);
        return this;
    }

    /**
     * Sets the map of {@link FencedLock} configurations, mapped by config
     * name. Names could optionally contain a {@link CPGroup} name, such as
     * "myLock@group1".
     *
     * @param lockConfigs the {@link FencedLock} config map to set
     * @return this config instance
     */
    public CPSubsystemConfig setLockConfigs(Map<String, FencedLockConfig> lockConfigs) {
        this.lockConfigs.clear();
        this.lockConfigs.putAll(lockConfigs);
        for (Entry<String, FencedLockConfig> entry : this.lockConfigs.entrySet()) {
            entry.getValue().setName(entry.getKey());
        }
        return this;
    }

    @Override
    public String toString() {
        return "CPSubsystemConfig{" + "cpMemberCount=" + cpMemberCount + ", groupSize=" + groupSize
                + ", sessionTimeToLiveSeconds=" + sessionTimeToLiveSeconds + ", sessionHeartbeatIntervalSeconds="
                + sessionHeartbeatIntervalSeconds + ", missingCPMemberAutoRemovalSeconds=" + missingCPMemberAutoRemovalSeconds
                + ", failOnIndeterminateOperationState=" + failOnIndeterminateOperationState + ", raftAlgorithmConfig="
                + raftAlgorithmConfig + ", semaphoreConfigs=" + semaphoreConfigs + ", lockConfigs=" + lockConfigs + '}';
    }

    @Override
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CPSubsystemConfig that = (CPSubsystemConfig) o;
        return cpMemberCount == that.cpMemberCount && groupSize == that.groupSize
                && sessionTimeToLiveSeconds == that.sessionTimeToLiveSeconds
                && sessionHeartbeatIntervalSeconds == that.sessionHeartbeatIntervalSeconds
                && missingCPMemberAutoRemovalSeconds == that.missingCPMemberAutoRemovalSeconds
                && failOnIndeterminateOperationState == that.failOnIndeterminateOperationState
                && persistenceEnabled == that.persistenceEnabled && dataLoadTimeoutSeconds == that.dataLoadTimeoutSeconds
                && Objects.equals(baseDir, that.baseDir)
                && Objects.equals(raftAlgorithmConfig, that.raftAlgorithmConfig)
                && Objects.equals(semaphoreConfigs, that.semaphoreConfigs)
                && Objects.equals(lockConfigs, that.lockConfigs)
                && Objects.equals(configPatternMatcher, that.configPatternMatcher);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cpMemberCount, groupSize, sessionTimeToLiveSeconds, sessionHeartbeatIntervalSeconds,
                missingCPMemberAutoRemovalSeconds, failOnIndeterminateOperationState, persistenceEnabled, baseDir,
                dataLoadTimeoutSeconds, raftAlgorithmConfig, semaphoreConfigs, lockConfigs, configPatternMatcher);
    }
}
