package com.hazelcast.spi.impl.operationexecutor.progressive;

class UnparkNode  {
    final PartitionQueue partitionQueue;

    int normalSize;
    int prioritySize;
    Object task;
    UnparkNode prev;
    boolean hasPriority;


    public UnparkNode(PartitionQueue partitionQueue, boolean hasPriority) {
        this.partitionQueue = partitionQueue;
        this.hasPriority = hasPriority;
    }

    @Override
    public String toString() {
        return "UnparkNode{" +
                "partitionQueue=" + partitionQueue +
                ", hasPriority=" + hasPriority +
                ", normalSize=" + normalSize +
                ", prioritySize=" + prioritySize +
                ", prev=" + prev +
                '}';
    }
}
