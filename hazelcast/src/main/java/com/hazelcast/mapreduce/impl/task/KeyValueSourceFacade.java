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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.logging.ILogger;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.operation.ProcessStatsUpdateOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * This class is a facade around the normal {@link com.hazelcast.mapreduce.KeyValueSource}
 * implementation defined in the job configuration to support updating the information
 * of processed records on the job owner.
 *
 * @param <K> type of the key
 * @param <V> type of the value
 */
class KeyValueSourceFacade<K, V>
        extends KeyValueSource<K, V> {

    private static final int UPDATE_PROCESSED_RECORDS_INTERVAL = 1000;

    private final ILogger logger;

    private final KeyValueSource<K, V> keyValueSource;
    private final JobSupervisor supervisor;

    private int processedRecords;

    KeyValueSourceFacade(KeyValueSource<K, V> keyValueSource, JobSupervisor supervisor) {
        this.keyValueSource = keyValueSource;
        this.supervisor = supervisor;
        this.logger = supervisor.getMapReduceService().getNodeEngine().getLogger(KeyValueSourceFacade.class);
    }

    @Override
    public boolean open(NodeEngine nodeEngine) {
        return keyValueSource.open(nodeEngine);
    }

    @Override
    public boolean hasNext() {
        return keyValueSource.hasNext();
    }

    @Override
    public K key() {
        K key = keyValueSource.key();
        processedRecords++;
        if (processedRecords == UPDATE_PROCESSED_RECORDS_INTERVAL) {
            notifyProcessStats();
            processedRecords = 0;
        }
        return key;
    }

    @Override
    public Map.Entry<K, V> element() {
        return keyValueSource.element();
    }

    @Override
    public boolean reset() {
        processedRecords = 0;
        return keyValueSource.reset();
    }

    @Override
    public boolean isAllKeysSupported() {
        return keyValueSource.isAllKeysSupported();
    }

    @Override
    protected Collection<K> getAllKeys0() {
        return keyValueSource.getAllKeys();
    }

    @Override
    public void close()
            throws IOException {
        notifyProcessStats();
        keyValueSource.close();
    }

    private void notifyProcessStats() {
        if (processedRecords > 0) {
            try {
                MapReduceService mapReduceService = supervisor.getMapReduceService();
                String name = supervisor.getConfiguration().getName();
                String jobId = supervisor.getConfiguration().getJobId();
                Address jobOwner = supervisor.getJobOwner();
                mapReduceService.processRequest(jobOwner, new ProcessStatsUpdateOperation(name, jobId, processedRecords), name);
            } catch (Exception ignore) {
                // Don't care if wasn't executed properly
                logger.finest("ProcessedRecords update couldn't be executed", ignore);
            }
        }
    }

}
