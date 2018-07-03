package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastTestSupport;

import java.io.Serializable;

public class Main extends HazelcastTestSupport {

    public static void main(String[] args) throws Exception {
        Config config = newConfig();

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);


        System.out.println("Inserting");
        for (int k = 0; k < 1000; k++) {
            hz1.getMap("somemap").put(k, "1");
        }

        System.out.println("Insertion completed");

        Thread.sleep(10000);
    }

    private static Config newConfig() {
        Config config = new Config();
        config.getGroupConfig().setName("dev");
        ManagementCenterConfig managementCenterConfig = config.getManagementCenterConfig();
        managementCenterConfig.setEnabled(true).setUrl("http://127.0.0.1:8080/hazelcast-mancenter/");
        config.setLicenseKey("HazelcastEnterpriseHD#2Nodes#maOHFiwR5YEcy1T6K7bJ0u290q21h9d19g00sX99C39399eG99Z9v0x9t9x0");
        // config.getMapConfig("somemap").setInMemoryFormat(InMemoryFormat.NATIVE).setBackupCount(0);
//        config.getNativeMemoryConfig().setEnabled(true).setSize(new MemorySize(5, MemoryUnit.GIGABYTES));
        return config;
    }

    public static class MyTask implements Runnable, Serializable {
        @Override
        public void run() {
        }
    }
}
