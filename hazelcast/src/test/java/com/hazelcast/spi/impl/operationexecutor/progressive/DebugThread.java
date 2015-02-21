package com.hazelcast.spi.impl.operationexecutor.progressive;

class DebugThread extends Thread {
    public static final int SCAN_DELAY = 5000;

    private final ProgressiveOperationExecutor scheduler;
    private final Node[] previousNodes;
    private volatile boolean shutdown;

    public DebugThread(ProgressiveOperationExecutor scheduler) {
        super("DebugThread");
        this.scheduler = scheduler;
        this.previousNodes = new Node[scheduler.partitionQueues.length];
    }

    public void shutdown() {
        shutdown = true;
        interrupt();
    }

    @Override
    public void run() {
        while (!shutdown) {
            detect(previousNodes);

            try {
                Thread.sleep(SCAN_DELAY);
            } catch (InterruptedException e) {
            }
        }
    }

    private void detect(Node[] previousNodes) {
        StringBuffer sb = new StringBuffer();
        sb.append("partitonqueues\n");
        for (int k = 0; k < scheduler.partitionQueues.length; k++) {
            PartitionQueue partitionQueue = scheduler.partitionQueues[k];
            Node node = partitionQueue.getHead();

            if (node.state != PartitionQueueState.Parked) {
                Node old = previousNodes[k];
                boolean stuck = false;
                if (old != null) {
                    if (node.normalSize > 0) {
                        stuck = node == old;
                    }
                }
                previousNodes[k] = node;

                sb.append('\t')
                        .append(partitionQueue.getPartitionId())
                        .append(" stuck=").append(stuck)
                        .append(" node=")
                        .append(partitionQueue.toString())
                        .append('\n');

                if (stuck) {
                    Thread stuckThread = partitionQueue.getScheduleQueue().getPartitionThread();
                    StackTraceElement[] stackTraceElements = stuckThread.getStackTrace();

                    sb.append("\tstuck thread: " + stuckThread.getName() + "\n");
                    //add each element of the stack trace
                    for (StackTraceElement element : stackTraceElements) {
                        sb.append("\t\t");
                        sb.append(element);
                        sb.append("\n");
                    }
                }
            }
        }

        sb.append("workqueues\n");
        for (ProgressiveScheduleQueue q : scheduler.scheduleQueues) {
            sb.append('\t')
                    .append(q.getPartitionThread().getName())
                    .append(" state=")
                    .append(q.head)
                    .append('\n');
        }

        scheduler.logger.info(sb.toString());
    }
}
