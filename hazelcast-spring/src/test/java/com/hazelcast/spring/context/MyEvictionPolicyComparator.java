package com.hazelcast.spring.context;

import com.hazelcast.internal.eviction.EvictableEntryView;
import com.hazelcast.internal.eviction.EvictionPolicyComparator;

public class MyEvictionPolicyComparator extends EvictionPolicyComparator {

    @Override
    public int compare(EvictableEntryView e1, EvictableEntryView e2) {
        return BOTH_OF_ENTRIES_HAVE_SAME_PRIORITY_TO_BE_EVICTED;
    }

}
