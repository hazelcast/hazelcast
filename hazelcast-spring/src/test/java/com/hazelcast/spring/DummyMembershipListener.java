package com.hazelcast.spring;

import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

public class DummyMembershipListener implements MembershipListener {

	public void memberAdded(MembershipEvent membershipEvent) {
		System.err.println(membershipEvent);
	}

	public void memberRemoved(MembershipEvent membershipEvent) {
		System.err.println(membershipEvent);
	}

}
