package com.hazelcast.client.executor.tasks;

import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;

public class SelectNoMembers implements MemberSelector {
    @Override
    public boolean select(Member member) {
        return false;
    }
}