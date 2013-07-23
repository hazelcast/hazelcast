package com.hazelcast.queue;

/**
 * @ali 7/23/13
 */
public class CheckAndEvictOperation extends QueueOperation {

    public CheckAndEvictOperation() {
    }

    public CheckAndEvictOperation(String name) {
        super(name);
    }

    public int getId() {
        return QueueDataSerializerHook.CHECK_EVICT;
    }

    public void run() throws Exception {
        final QueueContainer container = getOrCreateContainer();
        if (container.isEvictable()){
            getNodeEngine().getProxyService().destroyDistributedObject(getServiceName(), name);
        }
    }
}
