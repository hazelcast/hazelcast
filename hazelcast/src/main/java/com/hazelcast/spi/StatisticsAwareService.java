package com.hazelcast.spi;

import com.hazelcast.monitor.LocalInstanceStats;

import java.util.Map;

/**
 *
 * This interface is in BETA stage and is subject to change in upcoming releases.
 *
 */
public interface StatisticsAwareService {

    <T extends LocalInstanceStats> Map<String, T> getStats();

}
