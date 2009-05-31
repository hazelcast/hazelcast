/**
 * 
 */
package com.hazelcast.config;

import java.util.ArrayList;
import java.util.List;

public class JoinMembers {
    public int connectionTimeoutSeconds = 5;

    public boolean enabled = false;

    public List<String> lsMembers = new ArrayList<String>();

    public String requiredMember = null;

    public void add(final String member) {
        lsMembers.add(member);
    }
}