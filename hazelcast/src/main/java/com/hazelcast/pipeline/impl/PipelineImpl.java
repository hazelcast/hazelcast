package com.hazelcast.pipeline.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IMap;
import com.hazelcast.pipeline.Pipeline;

import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class PipelineImpl implements Pipeline {

    private final static Executor callerRuns = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    private final int size;
    private final Semaphore semaphore;

    public PipelineImpl(int size){
        this.size = size;
        this.semaphore = new Semaphore(size);
    }

    @Override
    public <E> ICompletableFuture<E> add(ICompletableFuture<E> f) throws InterruptedException {
        semaphore.acquire();
        f.andThen(new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                semaphore.release();
            }

            @Override
            public void onFailure(Throwable t) {
                semaphore.release();
            }
        },callerRuns);
        return f;
    }



    public void run()throws Exception{
        HazelcastInstance hz = null;
        Pipeline pipeline = hz.newPipeline(1000);
        IMap map = hz.getMap("foo");
        for(int k=0;k<100000;k++){
            pipeline.add(map.getAsync(k));
        }
        pipeline.awaitCompletion();
    }
}
