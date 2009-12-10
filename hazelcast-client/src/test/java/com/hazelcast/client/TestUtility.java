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
		String[] addresses = new String[h.length];
		for (int i = 0; i < h.length; i++) {
			addresses[i] = h[i].getCluster().getLocalMember().getInetSocketAddress().getHostName();
//            addresses[i] = h[i].getCluster().getLocalMember().getInetSocketAddress().getHostName() + ":" + h[i].getCluster().getLocalMember().getInetSocketAddress().getPort();

		}
        String name = h[0].getConfig().getGroupName();
        String pass = h[0].getConfig().getGroupPassword();
//		HazelcastClient client = HazelcastClient.getHazelcastClient(true, addresses);
        HazelcastClient client = HazelcastClient.newHazelcastClient(name, pass, true, addresses);

		return client;
	}

}
