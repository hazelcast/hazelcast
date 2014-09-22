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

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.impl.CombinerResultList;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.notification.ReducingFinishedNotification;
import com.hazelcast.nio.Address;
import com.hazelcast.util.ExceptionUtil;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.mapreduce.impl.MapReduceUtil.notifyRemoteException;

/**
 * This task implementation executes the reducing phase. It collects all arriving chunks and processes them
 * one by one. There is one ReducerTask per job per node to have a clear idea of the resulting load a job
 * may create.
 *
 * @param <Key> type of the emitted key
 * @param <Chunk> type of the intermediate chunk data
 */
public class ReducerTask<Key, Chunk>
        implements Runnable {

    private final AtomicBoolean cancelled = new AtomicBoolean();

    private final JobSupervisor supervisor;
    private final Queue<ReducerChunk<Key, Chunk>> reducerQueue;
    private final String name;
    private final String jobId;

    private AtomicBoolean active = new AtomicBoolean();

    public ReducerTask(String name, String jobId, JobSupervisor supervisor) {
        this.name = name;
        this.jobId = jobId;
        this.supervisor = supervisor;
        this.reducerQueue = new ConcurrentLinkedQueue<ReducerChunk<Key, Chunk>>();
    }

    public String getName() {
        return name;
    }

    public String getJobId() {
        return jobId;
    }

    public void cancel() {
        cancelled.set(true);
    }

    public void processChunk(Map<Key, Chunk> chunk) {
        processChunk(-1, null, chunk);
    }

    public void processChunk(int partitionId, Address sender, Map<Key, Chunk> chunk) {
        if (cancelled.get()) {
            return;
        }
        reducerQueue.offer(new ReducerChunk<Key, Chunk>(chunk, partitionId, sender));
        if (active.compareAndSet(false, true)) {
            MapReduceService mapReduceService = supervisor.getMapReduceService();
            ExecutorService es = mapReduceService.getExecutorService(name);
            es.submit(this);
        }
    }

    @Override
    public void run() {
        try {
            ReducerChunk<Key, Chunk> reducerChunk;
            while ((reducerChunk = reducerQueue.poll()) != null) {
                if (cancelled.get()) {
                    return;
                }

                reduceChunk(reducerChunk.chunk);
                processProcessedState(reducerChunk);
            }
        } catch (Throwable t) {
            notifyRemoteException(supervisor, t);
            if (t instanceof Error) {
                ExceptionUtil.sneakyThrow(t);
            }
        } finally {
            active.compareAndSet(true, false);
        }
    }

    private void reduceChunk(Map<Key, Chunk> chunk) {
        for (Map.Entry<Key, Chunk> entry : chunk.entrySet()) {
            Reducer reducer = supervisor.getReducerByKey(entry.getKey());
            if (reducer != null) {
                Chunk chunkValue = entry.getValue();
                if (chunkValue instanceof CombinerResultList) {
                    for (Object value : (List) chunkValue) {
                        reducer.reduce(value);
                    }
                } else {
                    reducer.reduce(chunkValue);
                }
            }
        }
    }

    private void processProcessedState(ReducerChunk<Key, Chunk> reducerChunk) {
        // If partitionId is set this was the last chunk for this partition
        if (reducerChunk.partitionId != -1) {
            MapReduceService mapReduceService = supervisor.getMapReduceService();
            ReducingFinishedNotification notification = new ReducingFinishedNotification(mapReduceService.getLocalAddress(), name,
                    jobId, reducerChunk.partitionId);

            mapReduceService.sendNotification(reducerChunk.sender, notification);
        }
    }

}
