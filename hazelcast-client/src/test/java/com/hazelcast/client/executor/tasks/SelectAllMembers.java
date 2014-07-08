package com.hazelcast.client.executor.tasks;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;

public class SelectAllMembers implements MemberSelector {
    @Override
    public boolean select(final Member member) {
        return true;
    }
}