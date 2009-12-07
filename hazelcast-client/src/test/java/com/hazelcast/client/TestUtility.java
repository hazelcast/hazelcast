package com.hazelcast.client;

import java.net.InetSocketAddress;

import com.hazelcast.core.HazelcastInstance;

public class TestUtility {
	public static HazelcastClient getHazelcastClient(HazelcastInstance ... h) {
		InetSocketAddress[] addresses = new InetSocketAddress[h.length];
		for (int i = 0; i < h.length; i++) {
			addresses[i] = h[i].getCluster().getLocalMember().getInetSocketAddress();
		}
		HazelcastClient client = HazelcastClient.getHazelcastClient(true, addresses);
		return client;
	}

}
