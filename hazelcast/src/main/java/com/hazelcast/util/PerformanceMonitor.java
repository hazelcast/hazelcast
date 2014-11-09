package com.hazelcast.util;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.OperationService;

import java.util.concurrent.TimeUnit;

public class PerformanceMonitor extends Thread {

    private final ILogger logger;
    private final Node node;
    private final int delaySeconds;
    private final OperationService operationService;
    private final ConnectionManager connectionManager;

    public PerformanceMonitor(HazelcastInstanceImpl hazelcastInstance,int delaySeconds) {
        super(hazelcastInstance.node.threadGroup, hazelcastInstance.node.getThreadNamePrefix("PerformanceMonitor"));
        setDaemon(true);

        this.delaySeconds = delaySeconds;
        this.node = hazelcastInstance.node;
        this.logger = node.getLogger(PerformanceMonitor.class.getName());
        this.operationService = node.nodeEngine.getOperationService();
        this.connectionManager = node.connectionManager;
    }

    @Override
    public void run() {
        StringBuffer sb = new StringBuffer();

        while (node.isActive()) {
            sb.append("\n");
            sb.append("ConnectionManager metrics\n");
            connectionManager.dumpPerformanceMetrics(sb);
            sb.append("OperationService metrics\n");
            operationService.dumpPerformanceMetrics(sb);

            logger.info(sb.toString());

            sb.setLength(0);
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(delaySeconds));
            } catch (InterruptedException e) {
                return;
            }
        }
    }

}
