package com.hazelcast.client;

import java.net.InetSocketAddress;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Hazelcast;
import static com.hazelcast.client.TestUtility.getHazelcastClient;

public class TestUtility {
    static HazelcastClient client;


    public static HazelcastClient getHazelcastClient(){
        if(client==null){
            client = getHazelcastClient(Hazelcast.newHazelcastInstance(null));
        }
        return client;
    }
    
	public static HazelcastClient getHazelcastClient(HazelcastInstance ... h) {
		InetSocketAddress[] addresses = new InetSocketAddress[h.length];
		for (int i = 0; i < h.length; i++) {
			addresses[i] = h[i].getCluster().getLocalMember().getInetSocketAddress();
		}
		HazelcastClient client = HazelcastClient.getHazelcastClient(true, addresses);
		return client;
	}

}
