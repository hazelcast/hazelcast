package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastTestSupport;

/**
 * Created by alarmnummer on 5/13/16.
 */
public class Main extends HazelcastTestSupport{

    public static void main(String[] args){
        setLoggingLog4j();

        Config config = new Config();
        config.addMapConfig(new MapConfig("foo").setStatisticsEnabled(false));

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        IMap map = hz1.getMap("foo");
        String key = generateKeyOwnedBy(hz1);

     //   map.put(key,new byte[1000]);

        map.get(generateKeyNotOwnedBy(hz1));
    }
}
