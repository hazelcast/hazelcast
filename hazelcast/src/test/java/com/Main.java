package com;

import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        Config config = new Config();
        config.getGroupConfig().setName("dev");
        ManagementCenterConfig managementCenterConfig = config.getManagementCenterConfig();
        managementCenterConfig.setEnabled(true).setUrl("http://127.0.0.1:8080/hazelcast-mancenter/");
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        int l=0;
        for (; ; ) {
            l++;
            for (int k = 1; k < 2; k++) {
                hz.getMap("foo" + k).put(l % 10000, 1);
                hz.getMap("foo"+k).get(l % 10000);
                Thread.sleep(100);
            }
        }
    }
}
