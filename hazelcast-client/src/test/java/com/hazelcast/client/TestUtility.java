package com.hazelcast.client;

import java.net.InetSocketAddress;

import com.hazelcast.core.HazelcastInstance;

public class TestUtility {
	public static HazelcastClient getHazelcastClient(HazelcastInstance ... h) {
		InetSocketAddress[] addresses = new InetSocketAddress[h.length];
		for (int i = 0; i < h.length; i++) {
			addresses[i] = new InetSocketAddress(h[i].getCluster().getLocalMember().getInetAddress(),h[i].getCluster().getLocalMember().getPort());
		}
		HazelcastClient client = HazelcastClient.getHazelcastClient(addresses);
		return client;
	}

}
