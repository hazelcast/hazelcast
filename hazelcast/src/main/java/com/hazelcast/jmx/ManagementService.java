/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */
package com.hazelcast.jmx;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.hazelcast.core.Cluster;

/**
 * The management service instruments Hazelcast with MBeans required to
 * use JMX monitoring tools.
 * 
 * Enabling JMX monitoring can have a heavy impact on a busy Hazelcast cluster,
 * so the classes are not instrumented by default.
 * To enable the JMX agent, set this system property when you start the JVM or Java application:
 *   -Dhazelcast.jmx=true
 * for compatibility reason, also -Dcom.sun.management.jmxremote start the agent.
 * 
 * @author Marco Ferrante, DISI - University of Genoa
 */
public class ManagementService {

	private final static Logger logger = Logger.getLogger(ManagementService.class.getName());
	
	public static String ENABLE_JMX = "hazelcast.jmx";
	
	private static DataMBean dataMonitor = null; 
	
	private static ScheduledThreadPoolExecutor statCollectors;
	
    /**
     * Register all the MBeans.
     */
    public static void register(Cluster cluster) {
    	if (!("TRUE".equalsIgnoreCase(System.getProperty(ENABLE_JMX))
    			|| System.getProperties().containsKey("com.sun.management.jmxremote"))) {
    		// JMX disabled
    		return;
    	}
    	logger.log(Level.INFO, "JMX agent enabled");

    	// Scheduler of the statistics collectors
		if (showDetails()) {
			if (statCollectors == null) {
				statCollectors = new ScheduledThreadPoolExecutor(4);
			}
		}
    	
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        
        // Register the cluster
		try {
			ClusterMBean clusterMBean = new ClusterMBean(cluster);
            mbs.registerMBean(clusterMBean, clusterMBean.getObjectName());    
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Unable to start JMX service", e);
			return;
		}
		
		// Register the data monitor
		try {
			if (dataMonitor == null) {
				dataMonitor = new DataMBean();
			}
    		mbs.registerMBean(dataMonitor, null);
		}
		catch (Exception e) {
			logger.log(Level.WARNING, "Unable to start JMX data instance service", e);
		} 
		
    }
    
    public static void shutdown() {
		if (dataMonitor != null) {
			dataMonitor = null;
		}

		// Remove all entries
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		Set<ObjectName> entries;
		try {
			entries = mbs.queryNames(new ObjectName(MBeanBuilder.NAME_DOMAIN + "*"), null);
			for (ObjectName name : entries) {
				mbs.unregisterMBean(name);
			}
		}
		catch (Exception e) {
			logger.log(Level.FINE, "Error unregistering MBeans", e);
		}
		
    	if (statCollectors != null) {
    		statCollectors.shutdownNow();
    	}

    }
    
    /**
     * Return if the instrumentation must manage objects and 
     * statistics at detailed level.
     * For forward compatibility, return always true.
     */
    protected static boolean showDetails() {
    	return true;
    }
    
    protected static DataMBean getDataMonitor() {
    	return dataMonitor;
    }
    
    protected static class ScheduledCollector implements Runnable, StatisticsCollector {
    	
    	private long interval = 1;  // sec
    	private volatile long events = 0;
    	private volatile long total = 0;
    	private volatile double min = Long.MAX_VALUE;
    	private volatile double max = 0;
    	private volatile double average = 0;

    	private ScheduledFuture<StatisticsCollector> future;
    	
    	public ScheduledCollector(long interval) {
    		this.interval = interval;
    	}
    	
    	public void run() {
    		synchronized (this) {
    			average = events / interval;
    			events = 0;
    			min = average < min ? average : min;
    			max = average > max ? average : max;
    		}
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
     * @return statisticsCollector
     */
    @SuppressWarnings("unchecked")
	public static StatisticsCollector newStatisticsCollector() {
    	if (statCollectors != null) {
    		long interval = 1L;
    		ScheduledCollector collector = new ScheduledCollector(interval);
     		ScheduledFuture future = statCollectors.scheduleWithFixedDelay(collector, interval, interval, TimeUnit.SECONDS);
     		collector.setScheduledFuture(future);
     		return collector;
    	}
    	else {
    		return null;
    	}
    }

}
