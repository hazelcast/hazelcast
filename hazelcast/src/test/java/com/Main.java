package com;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        System.setProperty("hazelcast.diagnostics.enabled", "true");
        System.setProperty("hazelcast.diagnostics.metric.distributed.datastructures", "true");
        System.setProperty("hazelcast.diagnostics.metric.level", "info");
        System.setProperty("hazelcast.diagnostics.directory", "log");
        System.setProperty("hazelcast.diagnostics.filename.prefix", "foo");

        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IScheduledExecutorService scheduler = instance.getScheduledExecutorService("banana");
        scheduler.scheduleOnAllMembers(new EchoTask("Member Task"), 0, TimeUnit.SECONDS);
        scheduler.schedule(new EchoTask("Other Task"), 0, TimeUnit.SECONDS);
        int totalTasksCount = 0;
        for (Map.Entry<Member, List<IScheduledFuture<Object>>> entry : scheduler.getAllScheduledFutures().entrySet()) {
            totalTasksCount += entry.getValue().size();
        }
        System.out.println("Total tasks count: " + totalTasksCount);
    }

}
