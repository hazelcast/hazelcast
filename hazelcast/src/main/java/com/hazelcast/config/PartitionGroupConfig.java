/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * With the PartitionGroupConfig you can control how primary and backups are going to be split up. In a
 * multi member cluster, Hazelcast will place the backup partition on a different member to the primary partition
 * to prevent data loss.
 * <p/>
 * But in some cases this is not good enough; imagine that you have a 4 member cluster. And imagine that member 1
 * and member 2 run in data center 1 and member 3 and member 5 in data center 2. If you want high availability, you want
 * to all backups of the primary partitions in data center 1 or stored in data center 2. And you want the same high
 * availability for the primary partitions in data center2.
 * <p/>
 * With the PartitionGroupConfig this behavior can be controlled. Imagine that members in data center 1 have ip
 * address 10.10.1.* and ip addresses of data center 2 is 10.10.2.* then you can make the following configuration:
 * <p/>
 * <code>
 * <partition-group enabled="true" group-type="CUSTOM">
 * <member-group>
 * <interface>10.10.1.*</interface>
 * </member-group>
 * <member-group>
 * <interface>10.10.2.*</interface>
 * </member-group>
 * </partition-group>
 * </code>
 * <p/>
 * The interfaces can be configured with wildcards '*' and ranges e.g. '10-20'. Each member-group can have as many
 * interfaces as you want.
 * <p/>
 * You can define as many groups as you want, Hazelcast will prevent that in a multi member cluster, backups are
 * going to be stored in the same group as the primary.
 * <p/>
 * You need to be careful with selecting overlapping groups, e.g.
 * <code>
 * <partition-group enabled="true" group-type="CUSTOM">
 * <member-group>
 * <interface>10.10.1.1</interface>
 * <interface>10.10.1.2</interface>
 * </member-group>
 * <member-group>
 * <interface>10.10.1.1</interface>
 * <interface>10.10.1.3</interface>
 * </member-group>
 * </partition-group>
 * </code>
 * In this example there are 2 groups, but because interface 10.10.1.1 is shared between the 2 groups, there is still
 * a chance that this member is going to store primary and backups.
 * <p/>
 * In the previous example we made use of custom group; we creates the groups manually and we configured the
 * interfaces for these groups. There also is another option that doesn't give you the same fine grained control,
 * but can prevent that on a single machine, running multiple HazelcastInstances, a primary and backup are going
 * to be stored. This can be done using the HOST_AWARE group-type:
 * <p/>
 * <code>
 * <partition-group enabled="true" group-type="HOST_AWARE"/>
 * </code>
 */
public class PartitionGroupConfig {

    private boolean enabled = false;

    private MemberGroupType groupType = MemberGroupType.PER_MEMBER;

    private final List<MemberGroupConfig> memberGroupConfigs = new LinkedList<MemberGroupConfig>();

    public enum MemberGroupType {
        HOST_AWARE, CUSTOM, PER_MEMBER
    }

    /**
     * Checks if this PartitionGroupConfig is enabled.
     *
     * @return true if enabled, false otherwise.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Enables or disables this PartitionGroupConfig.
     *
     * @param enabled true if enabled, false if disabled.
     * @return the updated PartitionGroupConfig.
     */
    public PartitionGroupConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Returns the MemberGroupType configured. Could be null if no MemberGroupType has been configured.
     *
     * @return the MemberGroupType.
     */
    public MemberGroupType getGroupType() {
        return groupType;
    }

    /**
     * Sets the MemberGroupType. A @{link MemberGroupType#CUSTOM} indicates that custom groups are created.
     * With the {@link MemberGroupType#HOST_AWARE} group type, Hazelcast makes a group for every host, that prevents
     * a single host containing primary and backup. See the {@see MemberGroupConfig} for more information.
     *
     * @param memberGroupType the MemberGroupType to set.
     * @return the updated PartitionGroupConfig
     * @throws IllegalArgumentException if memberGroupType is null.
     * @see #getGroupType()
     */
    public PartitionGroupConfig setGroupType(MemberGroupType memberGroupType) {
        this.groupType = isNotNull(memberGroupType, "memberGroupType");
        return this;
    }

    /**
     * Adds a {@link MemberGroupConfig}. Duplicate elements are not filtered.
     *
     * @param memberGroupConfig the MemberGroupConfig to add.
     * @return the updated PartitionGroupConfig
     * @throws IllegalArgumentException if memberGroupConfig is null.
     * @see #addMemberGroupConfig(MemberGroupConfig)
     */
    public PartitionGroupConfig addMemberGroupConfig(MemberGroupConfig memberGroupConfig) {
        memberGroupConfigs.add(isNotNull(memberGroupConfig, "MemberGroupConfig"));
        return this;
    }

    /**
     * Returns an unmodifiable collection containing all {@link MemberGroupConfig} elements.
     *
     * @return the MemberGroupConfig elements.
     * @see #setMemberGroupConfigs(java.util.Collection)
     */
    public Collection<MemberGroupConfig> getMemberGroupConfigs() {
        return Collections.unmodifiableCollection(memberGroupConfigs);
    }

    /**
     * Removes all the {@link MemberGroupType} instances.
     *
     * @return the updated PartitionGroupConfig.
     * @see #setMemberGroupConfigs(java.util.Collection)
     */
    public PartitionGroupConfig clear() {
        memberGroupConfigs.clear();
        return this;
    }

    /**
     * Adds a MemberGroupConfig. This MemberGroupConfig only has meaning when the group-type is set the
     * {@link MemberGroupType#CUSTOM}. See the {@link PartitionGroupConfig} for more information and examples
     * of how this mechanism works.
     *
     * @param memberGroupConfigs the collection of MemberGroupConfig to add.
     * @return the updated PartitionGroupConfig
     * @throws IllegalArgumentException if memberGroupConfigs is null.
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
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("PartitionGroupConfig");
        sb.append("{enabled=").append(enabled);
        sb.append(", groupType=").append(groupType);
        sb.append(", memberGroupConfigs=").append(memberGroupConfigs);
        sb.append('}');
        return sb.toString();
    }
}
