package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.connection.Connection;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionNotActiveException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.transaction.impl.TransactionLog;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;

import static com.hazelcast.transaction.impl.Transaction.State.*;

/**
 * @ali 6/6/13
 */
public class TransactionProxy implements Transaction {

    private static final ThreadLocal<Boolean> threadFlag = new ThreadLocal<Boolean>();

    private final TransactionOptions options;
    private final ClientClusterServiceImpl clusterService;
    private final long threadId = Thread.currentThread().getId();
    private final Connection connection;

    private String txnId;
    private State state = NO_TXN;
    private long startTime = 0L;

    public TransactionProxy(HazelcastClient client, TransactionOptions options, Connection connection) {
        this.options = options;
        this.clusterService = (ClientClusterServiceImpl) client.getClientClusterService();
        this.connection = connection;
    }

    public void addTransactionLog(TransactionLog transactionLog) {

    }

    public void removeTransactionLog(Object key) {

    }

    public TransactionLog getTransactionLog(Object key) {
        return null;
    }

    public String getTxnId() {
        return txnId;
    }

    public State getState() {
        return state;
    }

    public long getTimeoutMillis() {
        return options.getTimeoutMillis();
    }

    void begin() {
        try {
            if (state == ACTIVE) {
                throw new IllegalStateException("Transaction is already active");
            }
            checkThread();
            if (threadFlag.get() != null) {
                throw new IllegalStateException("Nested transactions are not allowed!");
            }
            threadFlag.set(Boolean.TRUE);
            startTime = Clock.currentTimeMillis();

            txnId = sendAndReceive(new CreateTransactionRequest(options));
            state = ACTIVE;
        } catch (Exception e){
            closeConnection();
            ExceptionUtil.rethrow(e);
        }
    }

    void commit() {
        try {
            if (state != ACTIVE) {
                throw new TransactionNotActiveException("Transaction is not active");
            }
            checkThread();
            checkTimeout();
            sendAndReceive(new CommitTransactionRequest());
            state = COMMITTED;
        } finally {
            closeConnection();
        }
    }

    void rollback() {
        try {
            if (state == NO_TXN || state == ROLLED_BACK) {
                throw new IllegalStateException("Transaction is not active");
            }
            checkThread();
            try {
                sendAndReceive(new RollbackTransactionRequest());
            } catch (Exception e) {
            }
            state = ROLLED_BACK;
        } finally {
            closeConnection();
        }
    }

    private void closeConnection(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void checkThread() {
        if (threadId != Thread.currentThread().getId()) {
            throw new IllegalStateException("Transaction cannot span multiple threads!");
        }
    }

    private void checkTimeout() {
        if (startTime + options.getTimeoutMillis() < Clock.currentTimeMillis()) {
            throw new TransactionException("Transaction is timed-out!");
        }
    }

    private <T> T sendAndReceive(Object request) {
        try {
            return clusterService.sendAndReceiveFixedConnection(connection, request);
        } catch (IOException e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
