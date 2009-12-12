/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.config;

import com.hazelcast.nio.Address;

import java.util.ArrayList;
import java.util.List;

public class JoinMembers {
    private int connectionTimeoutSeconds = 5;

    private boolean enabled = false;

    private List<String> members = new ArrayList<String>();

    private String requiredMember = null;

    private List<Address> addresses = new ArrayList<Address>();

    public void addMember(final String member) {
        members.add(member);
    }

    public void addAddress(Address address) {
        addresses.add(address);
    }

    public List<Address> getAddresses() {
        return addresses;
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
    public void setConnectionTimeoutSeconds(int connectionTimeoutSeconds) {
        this.connectionTimeoutSeconds = connectionTimeoutSeconds;
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
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @return the lsMembers
     */
    public List<String> getMembers() {
        return members;
    }

    /**
     * @param members the members to set
     */
    public void setMembers(List<String> members) {
        this.members = members;
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
    public void setRequiredMember(String requiredMember) {
        this.requiredMember = requiredMember;
    }
}
