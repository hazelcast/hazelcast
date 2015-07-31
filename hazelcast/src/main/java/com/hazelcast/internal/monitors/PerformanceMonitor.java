/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.monitors;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.concurrent.TimeUnit;

/**
 * The PerformanceMonitor is responsible for logging all kinds of performance related information. Currently it only
 * shows the read/write events per selector and the operations executed per operation-thread, but new kinds of behavior
 * will be added.
 * <p/>
 * This tool is currently used internally. External users should be experts. In the future it will become more useful
 * for regular developers. It is also likely that most of the metrics we collect will be exposed through JMX at some
 * point in time.
 */
public class PerformanceMonitor extends Thread {

    private final ILogger logger;
    private final Node node;
    private final int delaySeconds;
    private final InternalOperationService operationService;
    private final ConnectionManager connectionManager;

    public PerformanceMonitor(HazelcastInstanceImpl hazelcastInstance, int delaySeconds) {
        super(hazelcastInstance.node.getHazelcastThreadGroup().getInternalThreadGroup(),
                hazelcastInstance.node.getHazelcastThreadGroup().getThreadNamePrefix("PerformanceMonitor"));
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
