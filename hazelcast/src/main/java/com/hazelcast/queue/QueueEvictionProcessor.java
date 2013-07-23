package com.hazelcast.queue;

import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.util.scheduler.ScheduledEntryProcessor;

import java.util.Collection;

/**
 * @ali 7/23/13
 */
public class QueueEvictionProcessor implements ScheduledEntryProcessor<String, Void> {

    final NodeEngine nodeEngine;

    final QueueService service;

    public QueueEvictionProcessor(NodeEngine nodeEngine, QueueService service) {
        this.nodeEngine = nodeEngine;
        this.service = service;
    }

    public void process(EntryTaskScheduler<String, Void> scheduler, Collection<ScheduledEntry<String, Void>> entries) {
        if (entries.isEmpty()){
            return;
        }
        for (ScheduledEntry<String, Void> entry : entries) {
            String name = entry.getKey();
            int partitionId = nodeEngine.getPartitionService().getPartitionId(nodeEngine.toData(name));
            final Invocation inv = nodeEngine.getOperationService().createInvocationBuilder(QueueService.SERVICE_NAME, new CheckAndEvictOperation(entry.getKey()), partitionId).build();
            inv.invoke();
        }

    }
}
