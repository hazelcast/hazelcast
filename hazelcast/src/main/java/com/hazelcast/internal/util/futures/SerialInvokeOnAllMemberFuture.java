package com.hazelcast.internal.util.futures;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

/**
 * Serially invoke operation on all cluster members. Iterates member list from the oldest to the youngest member.
 * If there is a cluster topology change then it restarts. Other errors are propagated.
 *
 * @param <T>
 */
public class SerialInvokeOnAllMemberFuture<T> extends AbstractCompletableFuture<T> {
    public static ExceptionHandler IGNORE_CLUSTER_TOPOLOGY_CHANGES = new ExceptionHandler() {
        @Override
        public <T extends Throwable> void handle(T throwable) throws T {
            if (throwable instanceof WrongTargetException || throwable instanceof TargetNotMemberException) {
                return;
            }
            throw throwable;
        }
    };

    private final ExceptionHandler exceptionHandler;

    public SerialInvokeOnAllMemberFuture(OperationFactory operationFactory, NodeEngine nodeEngine,
                                         ExceptionHandler exceptionHandler, int maxRetries) {
        super(nodeEngine, nodeEngine.getLogger(SerialInvokeOnAllMemberFuture.class));
        this.exceptionHandler = exceptionHandler;

        OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();
        AllMembersFutureFactory<T> allMembersFutureFactory = new AllMembersFutureFactory<T>(operationFactory,
                operationService, clusterService, maxRetries);

        ICompletableFuture<T> future = allMembersFutureFactory.createFuture();
        register(future, allMembersFutureFactory);
    }

    private void register(ICompletableFuture<T> future, final AllMembersFutureFactory<T> allMembersFutureFactory) {
        future.andThen(new ExecutionCallback<T>() {
            @Override
            public void onResponse(T response) {
                try {
                    ICompletableFuture<T> nextFuture = allMembersFutureFactory.createFuture();
                    registerOrComplete(response, nextFuture, allMembersFutureFactory);
                } catch (HazelcastException e) {
                    setResult(e);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                try {
                    exceptionHandler.handle(t);
                    ICompletableFuture<T> nextFuture = allMembersFutureFactory.createFuture();
                    registerOrComplete(null, nextFuture, allMembersFutureFactory);
                } catch (Throwable throwable) {
                    setResult(throwable);
                }
            }
        });
    }

    private void registerOrComplete(T response, ICompletableFuture<T> currentFuture, AllMembersFutureFactory<T> allMembersFutureFactory) {
        if (currentFuture == null) {
            // we are done, let's set the result to ourselves
            setResult(response);
        } else {
            // there is still a next future
            register(currentFuture, allMembersFutureFactory);
        }
    }

    public interface ExceptionHandler {
        <T extends Throwable> void handle(T throwable) throws T;
    }
}
