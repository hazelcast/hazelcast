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

package com.hazelcast.config;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.internal.util.Preconditions.isNotNull;

/**
 * With PartitionGroupConfig, you can control how primary and backup partitions are mapped to physical Members.
 * <p>
 * Hazelcast will always place partitions on different partition groups so as to provide redundancy.
 * There are seven partition group schemes defined in {@link MemberGroupType}: PER_MEMBER, HOST_AWARE
 * CUSTOM, ZONE_AWARE, NODE_AWARE, PLACEMENT_AWARE, SPI.
 * <p>
 * In all cases a partition will never be created on the same group. If there are more partitions defined than
 * there are partition groups, then only those partitions, up to the number of partition groups, will be created.
 * For example, if you define 2 backups, then with the primary, that makes 3. If you have only two partition groups
 * only two will be created.
 *
 * <h1>PER_MEMBER Partition Groups</h1>
 * This is the default partition scheme and is used if no other scheme is defined.
 * Each Member is in a group of its own.
 * <p>
 * Partitions (primaries and backups) will be distributed randomly but not on the same Member.
 * <pre>
 * &lt;partition-group enabled="true" group-type="PER_MEMBER"/&gt;
 * </pre>
 *
 * This provides good redundancy when Members are on separate hosts but not if multiple instances are being
 * run from the same host.
 * <h1>HOST_AWARE Partition Groups</h1>
 * In this scheme, a group corresponds to a host, based on its IP address. Partitions will not be written to
 * any other members on the same host.
 * <p>
 * This scheme provides good redundancy when multiple instances are being run on the same host.
 * <pre>
 * &lt;partition-group enabled="true" group-type="HOST_AWARE"/&gt;
 * </pre>
 *
 * <h1>CUSTOM Partition Groups</h1>
 * In this scheme, IP addresses, or IP address ranges, are allocated to groups. Partitions are not written to the same
 * group. This is very useful for ensuring partitions are written to different racks or even availability zones.
 * <p>
 * For example, members in data center 1 have IP addresses in the range 10.10.1.* and for data center 2 they have
 * the IP address range 10.10.2.*. You would achieve HA by configuring a <code>CUSTOM</code> partition group as follows:
 * <pre>
 * &lt;partition-group enabled="true" group-type="CUSTOM"&gt;
 *      &lt;member-group&gt;
 *          &lt;interface&gt;10.10.1.*&lt;/interface&gt;
 *      &lt;/member-group&gt;
 *      &lt;member-group&gt;
 *          &lt;interface&gt;10.10.2.*&lt;/interface&gt;
 *      &lt;/member-group&gt;
 * &lt;/partition-group&gt;
 * </pre>
 *
 * The interfaces can be configured with wildcards ('*') and also with address ranges e.g. '10-20'. Each member-group
 * can have an unlimited number of interfaces.
 * <p>
 * You can define as many <code>member-group</code>s as you want. Hazelcast will always store backups in a different
 * member-group to the primary partition.
 *
 * <h1>ZONE_AWARE Partition Groups</h1>
 * In this scheme, groups are allocated according to the metadata provided by Discovery SPI.
 * These metadata are availability zone, rack and host. The backups of the partitions are not
 * placed on the same group so this is very useful for ensuring partitions are placed on
 * different availability zones without providing the IP addresses to the config ahead.
 * <code>
 * <pre>
 * &lt;partition-group enabled="true" group-type="ZONE_AWARE"/&gt;
 * </pre>
 * </code>
 *
 * <h1>NODE_AWARE Partition Groups</h1>
 * In this scheme, groups are allocated according to node name metadata provided by Discovery SPI.
 * For container orchestration tools like Kubernetes and Docker Swarm, node is the term used to refer
 * machine that containers/pods run on. A node may be a virtual or physical machine.
 * The backups of the partitions are not placed on same group so this is very useful for ensuring partitions
 * are placed on different nodes without providing the IP addresses to the config ahead.
 *
 * <code>
 * <pre>
 * &lt;partition-group enabled="true" group-type="NODE_AWARE"/&gt;
 * </pre>
 * </code>
 *
 * <h1>PLACEMENT_AWARE Partition Groups</h1>
 * In this scheme, groups are allocated according to the placement metadata provided by Discovery
 * SPI. Depending on the cloud provider, this metadata indicates the placement information (rack,
 * fault domain, etc.) of a VM in a zone. This scheme provides a finer granularity than ZONE_AWARE
 * for partition groups and is useful to provide good redundancy when running members within a
 * single availability zone.
 *
 * <code>
 * <pre>
 * &lt;partition-group enabled="true" group-type="PLACEMENT_AWARE"/&gt;
 * </pre>
 * </code>
 *
 * <h1>SPI Aware Partition Groups</h1>
 * In this scheme, groups are allocated according to the implementation provided by Discovery SPI.
 * <code>
 * <pre>
 * &lt;partition-group enabled="true" group-type="SPI"/&gt;
 * </pre>
 *
 * <h2>Overlapping Groups</h2>
 * Care should be taken when selecting overlapping groups, e.g.
 * <pre>
 * &lt;partition-group enabled="true" group-type="CUSTOM"&gt;
 *      &lt;member-group&gt;
 *          &lt;interface&gt;10.10.1.1&lt;/interface&gt;
 *          &lt;interface&gt;10.10.1.2&lt;/interface&gt;
 *      &lt;/member-group&gt;
 *      &lt;member-group&gt;
 *          &lt;interface&gt;10.10.1.1&lt;/interface&gt;
 *          &lt;interface&gt;10.10.1.3&lt;/interface&gt;
 *      &lt;/member-group&gt;
 * &lt;/partition-group&gt;
 * </pre>
 *
 * In this example there are 2 groups, but because interface 10.10.1.1 is shared between the 2 groups, this  member
 * may store store primary and backups.
 */
public class PartitionGroupConfig {

    private boolean enabled;

    private MemberGroupType groupType = MemberGroupType.PER_MEMBER;

    private final List<MemberGroupConfig> memberGroupConfigs = new LinkedList<MemberGroupConfig>();

    /**
     * Type of member groups.
     */
    public enum MemberGroupType {
        /**
         * Host aware.
         */
        HOST_AWARE,
        /**
         * Custom.
         */
        CUSTOM,
        /**
         * Per member.
         */
        PER_MEMBER,
        /**
         * Zone Aware. Backups will be created in other zones.
         * If only one zone is available, backups will be created in the same zone.
         */
        ZONE_AWARE,
        /**
         * Node Aware. Backups will be created in other nodes.
         * If only one node is available, backups will be created in the same node.
         */
        NODE_AWARE,
        /**
         * Placement Aware. Backups will be created in other placement groups.
         * If only one placement group is available, backups will be created in the same group.
         */
        PLACEMENT_AWARE,
        /**
         * MemberGroup implementation will be provided by the user via Discovery SPI.
         */
        SPI
    }

    /**
     * Checks if this PartitionGroupConfig is enabled.
     *
     * @return {@code true} if this PartitionGroupConfig is enabled, {@code false} otherwise
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables this PartitionGroupConfig.
     *
     * @param enabled {@code true} to enable, {@code false} to disable
     * @return the updated PartitionGroupConfig
     */
    public PartitionGroupConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the MemberGroupType configured. Could be {@code null} if no MemberGroupType has been configured.
     *
     * @return the MemberGroupType
     */
    public MemberGroupType getGroupType() {
        return groupType;
    }

    /**
     * Sets the MemberGroupType. A @{link MemberGroupType#CUSTOM} indicates that custom groups are created.
     * With the {@link MemberGroupType#HOST_AWARE} group type, Hazelcast makes a group for every host, that prevents
     * a single host containing primary and backup. See the {@link MemberGroupConfig} for more information.
     *
     * @param memberGroupType the MemberGroupType to set
     * @return the updated PartitionGroupConfig
     * @throws IllegalArgumentException if memberGroupType is {@code null}
     * @see #getGroupType()
     */
    public PartitionGroupConfig setGroupType(MemberGroupType memberGroupType) {
        this.groupType = isNotNull(memberGroupType, "memberGroupType");
        return this;
    }

    /**
     * Adds a {@link MemberGroupConfig}. Duplicate elements are not filtered.
     *
     * @param memberGroupConfig the MemberGroupConfig to add
     * @return the updated PartitionGroupConfig
     * @throws IllegalArgumentException if memberGroupConfig is {@code null}
     * @see #addMemberGroupConfig(MemberGroupConfig)
     */
    public PartitionGroupConfig addMemberGroupConfig(MemberGroupConfig memberGroupConfig) {
        memberGroupConfigs.add(isNotNull(memberGroupConfig, "memberGroupConfig"));
        return this;
    }

    /**
     * Returns an unmodifiable collection containing all {@link MemberGroupConfig} elements.
     *
     * @return an unmodifiable collection containing all {@link MemberGroupConfig} elements
     * @see #setMemberGroupConfigs(java.util.Collection)
     */
    public Collection<MemberGroupConfig> getMemberGroupConfigs() {
        return Collections.unmodifiableCollection(memberGroupConfigs);
    }

    /**
     * Removes all the {@link MemberGroupType} instances.
     *
     * @return the updated PartitionGroupConfig
     * @see #setMemberGroupConfigs(java.util.Collection)
     */
    public PartitionGroupConfig clear() {
        memberGroupConfigs.clear();
        return this;
    }

    /**
     * Adds a MemberGroupConfig. This MemberGroupConfig only has meaning when the group-type is set to
     * {@link MemberGroupType#CUSTOM}. See the {@link PartitionGroupConfig} for more information and examples
     * of how this mechanism works.
     *
     * @param memberGroupConfigs the collection of MemberGroupConfig to add
     * @return the updated PartitionGroupConfig
     * @throws IllegalArgumentException if memberGroupConfigs is {@code null}
     * @see #getMemberGroupConfigs()
     * @see #clear()
     * @see #addMemberGroupConfig(MemberGroupConfig)
     */
    public PartitionGroupConfig setMemberGroupConfigs(Collection<MemberGroupConfig> memberGroupConfigs) {
        isNotNull(memberGroupConfigs, "memberGroupConfigs");

        this.memberGroupConfigs.clear();
        this.memberGroupConfigs.addAll(memberGroupConfigs);
        return this;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof PartitionGroupConfig)) {
            return false;
        }

        PartitionGroupConfig that = (PartitionGroupConfig) o;

        if (enabled != that.enabled) {
            return false;
        }
        if (groupType != that.groupType) {
            return false;
        }
        return memberGroupConfigs.equals(that.memberGroupConfigs);
    }

    @Override
    public final int hashCode() {
        int result = (enabled ? 1 : 0);
        result = 31 * result + (groupType != null ? groupType.hashCode() : 0);
        result = 31 * result + memberGroupConfigs.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PartitionGroupConfig{"
                + "enabled=" + enabled
                + ", groupType=" + groupType
                + ", memberGroupConfigs=" + memberGroupConfigs
                + '}';
    }
}
