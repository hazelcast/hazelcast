/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jmx;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.executor.PoolExecutorThreadFactory;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

/**
 * The management service instruments Hazelcast with MBeans required to
 * use JMX monitoring tools.
 * <p/>
 * Enabling JMX monitoring can have a heavy impact on a busy Hazelcast cluster,
 * so the classes are not instrumented by default.
 * To enable the JMX agent, set this system property when you start the JVM or Java application:
 * -Dhazelcast.jmx=true
 * for compatibility reason, also -Dcom.sun.management.jmxremote start the agent.
 *
 * @author Marco Ferrante, DISI - University of Genoa
 */
@SuppressWarnings("SynchronizedMethod")
public class ManagementService {

    private static volatile ScheduledThreadPoolExecutor statCollectorExecutor;

    private final ILogger logger;

    private final HazelcastInstance instance;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private final String name;

    private final boolean enabled;

    private final boolean showDetails;

    public ManagementService(HazelcastInstanceImpl instance) {
        this.instance = instance;
        this.name = instance.getName();
        this.logger = instance.node.getLogger(ManagementService.class.getName());
        this.enabled = instance.node.groupProperties.ENABLE_JMX.getBoolean();
        this.showDetails = instance.node.groupProperties.ENABLE_JMX_DETAILED.getBoolean();
    }

    public HazelcastInstance getInstance() {
        return instance;
    }

    private static synchronized void startStatsCollector() {
        if (statCollectorExecutor == null) {
            statCollectorExecutor = new ScheduledThreadPoolExecutor(2,
                    new PoolExecutorThreadFactory(null, null, "hz.jmx", null));
        }
    }

    private static synchronized void stopStatsCollector() {
        if (statCollectorExecutor != null) {
            statCollectorExecutor.shutdownNow();
            statCollectorExecutor = null;
        }
    }

    /**
     * Register all the MBeans.
     */
    public void register() {
        if (!enabled) {
            return;
        }
        if (started.compareAndSet(false, true)) {
            logger.log(Level.INFO, "Hazelcast JMX agent enabled");
            // Scheduler of the statistics collectors
            if (showDetails()) {
                startStatsCollector();
            }
            MBeanServer mbs = mBeanServer();
            // Register the cluster monitor
            try {
                ClusterMBean clusterMBean = new ClusterMBean(this, this.name);
                mbs.registerMBean(clusterMBean, clusterMBean.getObjectName());
                DataMBean dataMBean = new DataMBean(this);
                dataMBean.setParentName(clusterMBean.getRootName());
                mbs.registerMBean(dataMBean, dataMBean.getObjectName());
            } catch (Exception e) {
                logger.log(Level.WARNING, "Unable to start JMX service", e);
            }
        }
    }

    /**
     * Unregister a cluster instance.
     */
    public void unregister() {
        if (!enabled) {
            return;
        }
        if (started.compareAndSet(true, false)) {
            // Remove all entries register for the cluster
            MBeanServer mbs = mBeanServer();
            try {
                Set<ObjectName> entries = mbs.queryNames(new ObjectName(
                        ObjectNameSpec.NAME_DOMAIN + "Cluster=" + name + ",*"), null);
                for (ObjectName name : entries) {
                    // Double check, in case the entry has been removed whiletime
                    if (mbs.isRegistered(name)) {
                        mbs.unregisterMBean(name);
                    }
                }
            } catch (Exception e) {
                logger.log(Level.FINEST, "Error unregistering MBeans", e);
            }
        }
    }

    private static MBeanServer mBeanServer() {
        return ManagementFactory.getPlatformMBeanServer();
    }

    /**
     * Stop the management service
     */
    public synchronized static void shutdown() {
        MBeanServer mbs = mBeanServer();
        try {
            Set<ObjectName> entries = mbs.queryNames(new ObjectName(ObjectNameSpec.NAME_DOMAIN + "*"), null);
            for (ObjectName name : entries) {
                // Double check, in case the entry has been removed in the meantime
                if (mbs.isRegistered(name)) {
                    mbs.unregisterMBean(name);
                }
            }
        } catch (Exception e) {
            Logger.getLogger("hz.ManagementCenter").log(Level.FINEST, "Error unregistering MBeans", e);
        }
        stopStatsCollector();
    }

    /**
     * Return if the instrumentation must manage objects and
     * statistics at detailed level.
     * For forward compatibility, return always true.
     */
    final boolean showDetails() {
        return showDetails;
    }

    @SuppressWarnings("VolatileLongOrDoubleField")
    protected static class ScheduledCollector implements Runnable, StatisticsCollector {

        private final long interval;  // sec
        private volatile long events = 0;
        private volatile long total = 0;
        private volatile double min = Long.MAX_VALUE;
        private volatile double max = 0;
        private volatile double average = 0;

        private ScheduledFuture<StatisticsCollector> future;

        public ScheduledCollector(long interval) {
            this.interval = interval;
        }

        public synchronized void run() {
            //noinspection SynchronizeOnThis
            average = (double) events / interval;
            events = 0;
            min = average < min ? average : min;
            max = average > max ? average : max;
        }

        private void setScheduledFuture(ScheduledFuture<StatisticsCollector> future) {
            this.future = future;
        }

        public void destroy() {
            future.cancel(true);
        }

        public synchronized void addEvent() {
            events++;
            total++;
        }

        public synchronized void reset() {
            events = 0;
            min = Long.MAX_VALUE;
            max = 0;
            average = 0;
        }

        public long getEvents() {
            return events;
        }

        public long getTotal() {
            return total;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }

        public double getAverage() {
            return average;
        }

        public long getInterval() {
            return interval;
        }
    }

    /**
     * Create a new collector or return null if statistics are not enabled
     *
     * @return statisticsCollector
     */
    @SuppressWarnings("unchecked")
    public static StatisticsCollector newStatisticsCollector() {
        final ScheduledExecutorService scheduledExecutor = statCollectorExecutor;
        if (scheduledExecutor != null) {
            long interval = 1L;
            ScheduledCollector collector = new ScheduledCollector(interval);
            ScheduledFuture future = scheduledExecutor.scheduleWithFixedDelay(collector, interval, interval, TimeUnit.SECONDS);
            collector.setScheduledFuture(future);
            return collector;
        } else {
            return null;
        }
    }
}
