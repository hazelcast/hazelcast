package com.hazelcast.queue.proxy;

import com.hazelcast.config.QueueConfig;
import com.hazelcast.nio.Data;
import com.hazelcast.queue.OfferOperation;
import com.hazelcast.queue.PeekOperation;
import com.hazelcast.queue.QueueService;
import com.hazelcast.queue.QueueSizeOperation;
import com.hazelcast.spi.Invocation;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.NodeService;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: ali
 * Date: 11/14/12
 * Time: 12:47 AM
 * To change this template use File | Settings | File Templates.
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

    protected boolean offerInternal(Data data, long ttl, TimeUnit timeUnit) {
        try {
            int backupCount = getBackupCount();
            boolean result = true;
            final Future[] futures = new Future[backupCount+1];
            for(int i=0; i <= backupCount; i++){
                OfferOperation offerOperation = new OfferOperation(name, data);
                offerOperation.setValidateTarget(true);
                offerOperation.setServiceName(QueueService.NAME);
                InvocationBuilder builder = nodeService.createInvocationBuilder(QueueService.NAME, offerOperation, getPartitionId());
                builder.setReplicaIndex(i);
                Invocation inv =  builder.build();
                futures[i] = inv.invoke();
            }
            for (Future f: futures){
                Object r = f.get();
                result = result && (Boolean)nodeService.toObject(r);
                if(!result){
                    break;
                }
            }
            return result;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public int size() {
        QueueSizeOperation sizeOperation = new QueueSizeOperation(name);
        sizeOperation.setValidateTarget(true);
        sizeOperation.setServiceName(QueueService.NAME);
        try {
            Invocation invocation = nodeService.createInvocationBuilder(QueueService.NAME, sizeOperation, getPartitionId()).build();
            Future future = invocation.invoke();
            Object result = future.get();
            return (Integer) nodeService.toObject(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    protected Data peekInternal(boolean poll){
        try {
            int backupCount = getBackupCount();
            final Future<Data>[] futures = new Future[backupCount+1];
            for(int i=0; i <= backupCount; i++){
                PeekOperation peekOperation = new PeekOperation(name, poll);
                peekOperation.setValidateTarget(true);
                peekOperation.setServiceName(QueueService.NAME);
                InvocationBuilder builder = nodeService.createInvocationBuilder(QueueService.NAME, peekOperation, getPartitionId());
                builder.setReplicaIndex(i);
                Invocation inv = builder.build();
                futures[i] = inv.invoke();
            }
            Data result = null;
            for(int i=0; i <= backupCount; i++){
                Data r = futures[i].get();
                if(i == 0){
                    result = r;
                }
            }
            return result;
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }


    private int getPartitionId() {
        return partitionId;
    }

    private int getBackupCount(){
        int queueBackupCount = config.getBackupCount();
        return Math.min(nodeService.getCluster().getMembers().size() - 1, queueBackupCount);
    }


}
