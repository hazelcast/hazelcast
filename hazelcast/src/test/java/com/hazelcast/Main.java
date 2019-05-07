package com.hazelcast;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.diagnostics.OperationQueueSamplePlugin;

public class Main {

    public static void main(String[] args) {

        MapConfig mapConfig = new MapConfig("foo").setBackupCount(0).setAsyncBackupCount(1);
        Config config = new Config().addMapConfig(mapConfig);
        config.setProperty(OperationQueueSamplePlugin.SAMPLE_PERIOD_SECONDS.getName(), "10");

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config);

        for (int k = 0; k < 10; k++) {
            new Thread() {
                public void run() {
                    try {
                        int l = 0;
                        for (; ; ) {
                            hz1.getMap("foo").put((l++) % 10000, new byte[100]);
                            l++;

                            if(l%100000==0){
                                System.out.println(getName()+" at "+l);
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

    }
}
