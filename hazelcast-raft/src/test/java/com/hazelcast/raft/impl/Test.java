package com.hazelcast.raft.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ServiceConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.raft.RaftConfig;
import com.hazelcast.raft.RaftMember;
import com.hazelcast.raft.impl.service.RaftService;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.concurrent.ExecutionException;

/**
 * TODO: Javadoc Pending...
 *
 */
public class Test {

    static {
        System.setProperty(GroupProperty.LOGGING_TYPE.getName(), "log4j2");
        System.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1");

        RaftConfig raftConfig = new RaftConfig();
        for (int i = 0; i < 3; i++) {
            raftConfig.addMember(new RaftMember("127.0.0.1:" + (5701 + i), "id-" + i));
        }

        config.getServicesConfig().addServiceConfig(
                new ServiceConfig().setEnabled(true).setName(RaftService.SERVICE_NAME)
                        .setClassName(RaftService.class.getName())
                        .setConfigObject(raftConfig));

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        RaftNode node = RaftUtil.getRaftNode(instance, "METADATA");
        RaftUtil.waitUntilLeaderElected(node);

        for (int i = 0; i < 10000; i++) {
            String s = String.valueOf(i);
//            Future replicate = node.replicate(s);
//            try {
//                System.out.println("---------------------------> DONE " + replicate.get());
//            } catch (Exception e) {
//                System.err.println("============================" + e.getMessage());
//            }
            Thread.sleep(1000);
        }
    }
}
