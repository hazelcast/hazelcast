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

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Contains the configuration for the tcp-ip join mechanism.
 */
public class TcpIpConfig {

    private int connectionTimeoutSeconds = 5;

    private boolean enabled = false;

    private List<String> members = new ArrayList<String>();

    private String requiredMember = null;

    /**
     * Adds a 'well known' member.
     *
     * Each HazelcastInstance will try to connect to at least one of the members to find all other members
     * and create a cluster.
     *
     * @param member the member to add.
     * @return the updated configuration.
     * @see #getMembers()
     */
    public TcpIpConfig addMember(final String member) {
        this.members.add(member);
        return this;
    }

    /**
     * Removes all members.
     *
     * Can safely be called when there are no members.
     *
     * @return the updated configuration.
     * @see #addMember(String)
     */
    public TcpIpConfig clear() {
        members.clear();
        return this;
    }

    /**
     * @return the connectionTimeoutSeconds
     */
    public int getConnectionTimeoutSeconds() {
        return connectionTimeoutSeconds;
    }

    /**
     * @param connectionTimeoutSeconds the connectionTimeoutSeconds to set
     */
    public TcpIpConfig setConnectionTimeoutSeconds(final int connectionTimeoutSeconds) {
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
        return this;
    }

    /**
     * @return the enabled
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * @param enabled the enabled to set
     */
    public TcpIpConfig setEnabled(final boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * @return the lsMembers
     */
    public List<String> getMembers() {
        if (members == null) {
            members = new ArrayList<String>();
        }
        return members;
    }

    /**
     * @param members the members to set
     */
    public TcpIpConfig setMembers(final List<String> members) {
        this.members.clear();
        for (String member : members) {
            StringTokenizer tokenizer = new StringTokenizer(member, ",");
            while (tokenizer.hasMoreTokens()) {
                String s = tokenizer.nextToken();
                this.members.add(s.trim());
            }
        }
        return this;
    }

    /**
     * @return the requiredMember
     */
    public String getRequiredMember() {
        return requiredMember;
    }

    /**
     * @param requiredMember the requiredMember to set
     */
    public TcpIpConfig setRequiredMember(final String requiredMember) {
        this.requiredMember = requiredMember;
        return this;
    }

    @Override
    public String toString() {
        return "TcpIpConfig [enabled=" + enabled
                + ", connectionTimeoutSeconds=" + connectionTimeoutSeconds
                + ", members=" + members
                + ", requiredMember=" + requiredMember
                + "]";
    }
}
