package com.hazelcast.util;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.OperationService;

import java.util.concurrent.TimeUnit;

/**
 * The PerformanceMonitor is responsible for logging all kinds of performance related information. Currently it only
 * shows the read/write events per selector and the operations executed per operation-thread, but new kinds of behavior
 * will be added.
 *
 * This tool is currently mostly used internally or unless you are an expert. In the future it will become more useful
 * for regular developers. It is also likely that most of the metrics we collect will be exposed through JMX at some
 * point in time.
 */
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
