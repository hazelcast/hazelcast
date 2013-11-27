package com.hazelcast.client.clientMembershipTests;

import com.hazelcast.core.MembershipListener;
import com.hazelcast.core.MembershipEvent;

/**
 * User: danny Date: 11/27/13
 */

class CountMemberListener implements MembershipListener {

    private int count;

    public CountMemberListener(){
        count=0;
    }

    public void memberAdded(MembershipEvent event){
        count++;
    }

    public void memberRemoved(MembershipEvent event){
        count--;
    }

    public int getRunningCount(){return count;}
}
