package com.hazelcast.client.pipeline;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.pipeline.Pipeline;
import com.hazelcast.pipeline.impl.FlushThreadLocal;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

public class PipelineImpl<E> implements Pipeline<E> {

    public final static boolean AUTO_FLUSH = Boolean.parseBoolean(System.getProperty("hz.pipeline.autoflush","false"));

    private final static Executor callerRuns = new Executor() {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    };

    private final int size;
    private final Semaphore semaphore;
    private final List<ICompletableFuture<E>> futures = new ArrayList<ICompletableFuture<E>>();
    private final ClientConnectionManager clientConnectionManager;

    public PipelineImpl(int size, ClientConnectionManager clientConnectionManager){
        if(!AUTO_FLUSH){
            FlushThreadLocal.flush(false);
        }
        this.size = size;
        this.semaphore = new Semaphore(size);
        this.clientConnectionManager = clientConnectionManager;
    }

    @Override
    public List<E> results() throws Exception{
        for(ClientConnection connection:clientConnectionManager.getActiveConnections()){
            connection.getChannel().flush();
        }
        FlushThreadLocal.flush(true);

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
