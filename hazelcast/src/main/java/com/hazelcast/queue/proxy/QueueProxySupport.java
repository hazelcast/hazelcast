package com.hazelcast.queue.proxy;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.Data;
import com.hazelcast.queue.*;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * User: ali
 * Date: 11/14/12
 * Time: 12:47 AM
 */
abstract class QueueProxySupport {

    protected final String name;
    protected final QueueService queueService;
    protected final NodeService nodeService;
    protected final int partitionId;
    protected final QueueConfig config;

    protected QueueProxySupport(final String name, final QueueService queueService, NodeService nodeService) {
        this.name = name;
        this.queueService = queueService;
        this.nodeService = nodeService;
        this.partitionId = nodeService.getPartitionId(nodeService.toData(name));
        this.config = nodeService.getConfig().getQueueConfig(name);
    }

    protected boolean offerInternal(Data data) {
        checkNull(data);
        try {
            OfferOperation operation = new OfferOperation(name, data);
            Invocation inv = nodeService.createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean) nodeService.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public int size() {
        try {
            QueueSizeOperation operation = new QueueSizeOperation(name);
            Invocation invocation = nodeService.createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future future = invocation.invoke();
            Object result = future.get();
            return (Integer) nodeService.toObject(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    public void clear() {
        try {
            ClearOperation operation = new ClearOperation(name);
            Invocation invocation = nodeService.createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future future = invocation.invoke();
            future.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected Data peekInternal() {
        try {
            PeekOperation operation = new PeekOperation(name);
            Invocation inv = nodeService.createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future<Data> f = inv.invoke();
            return f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    protected Data pollInternal(){
        try {
            PollOperation operation = new PollOperation(name);
            Invocation inv = nodeService.createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future<Data> f = inv.invoke();
            return f.get();
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    protected boolean removeInternal(Data data){
        try {
            RemoveOperation operation = new RemoveOperation(name, data);
            Invocation inv = nodeService.createInvocationBuilder(QueueService.NAME, operation, getPartitionId()).build();
            Future f = inv.invoke();
            return (Boolean)nodeService.toObject(f.get());
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    private int getPartitionId() {
        return partitionId;
    }

    private void checkNull(Data data){
        if(data == null){
            throw new NullPointerException();
        }
    }


}
