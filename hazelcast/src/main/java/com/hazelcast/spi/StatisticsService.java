package com.hazelcast.spi;

import com.hazelcast.monitor.LocalInstanceStats;

import java.util.Map;

/**
 * An interface to give SPI services ability to publish their statistics.
 */
public interface StatisticsService {

    <T extends LocalInstanceStats> Map<String, T> getStats();

}
