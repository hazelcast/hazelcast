package com.hazelcast;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ITopic;
import com.hazelcast.test.HazelcastTestSupport;

/**
 * Created by alarmnummer on 1/13/16.
 */
public class Main extends HazelcastTestSupport{
    public static void main(String[] args){
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        String topicName = generateKeyOwnedBy(hz2);
        ITopic topic = hz1.getTopic("foo");

        for(int k=0;k<1000 * 1000;k++){
            topic.publish("");

            if(k% 10000 == 0){
                System.out.println("at: "+k);
            }
        }

        System.out.println("done");
    }
}
