package com.hazelcast.pipeline.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.pipeline.Pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

public class PipelineImpl<E> implements Pipeline<E> {

    private final static Executor callerRuns = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    private final int size;
    private final Semaphore semaphore;
    private final List<ICompletableFuture<E>> futures = new ArrayList<ICompletableFuture<E>>();

    public PipelineImpl(int size){
        this.size = size;
        this.semaphore = new Semaphore(size);
    }

    @Override
    public List<E> results() throws Exception{
        List<E> result = new ArrayList<E>(futures.size());
        for(ICompletableFuture<E> f: futures){
            result.add(f.get());
        }
        return result;
    }

    @Override
    public ICompletableFuture<E> add(ICompletableFuture<E> f) throws InterruptedException {
        semaphore.acquire();
        futures.add(f);
        f.andThen(new ExecutionCallback<E>() {
            @Override
            public void onResponse(E response) {
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        },callerRuns);
        return f;
    }

}
