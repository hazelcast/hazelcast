package com;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.spi.properties.GroupProperty;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        System.setProperty(GroupProperty.SCHEDULED_EXECUTOR_LOG_DELAY_SECONDS.getName(),"1");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(new Config());
        IScheduledExecutorService scheduler = instance.getScheduledExecutorService("banana");
        scheduler.scheduleOnAllMembers(new EchoTask("Member Task"), 10, TimeUnit.SECONDS);
        scheduler.schedule(new EchoTask("Other Task"), 10, TimeUnit.SECONDS);

        for(int k=0;k<200;k++){
            scheduler.schedule(new EchoTask("foobar"),10,TimeUnit.SECONDS);
        }

        int totalTasksCount = 0;
        for (Map.Entry<Member, List<IScheduledFuture<Object>>> entry : scheduler.getAllScheduledFutures().entrySet()) {
            totalTasksCount += entry.getValue().size();
        }
        System.out.println("Total tasks count: " + totalTasksCount);
    }

}
