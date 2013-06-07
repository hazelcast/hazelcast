package com.hazelcast.client.txn;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.transaction.Transaction;
import com.hazelcast.transaction.TransactionLog;
import com.hazelcast.transaction.TransactionOptions;

/**
 * @ali 6/6/13
 */
public class TransactionProxy implements Transaction {

    final HazelcastClient client;
    final TransactionOptions options;
    final ClientClusterServiceImpl clusterService;
    String txnId;

    public TransactionProxy(HazelcastClient client, TransactionOptions options){
        this.client = client;
        this.options = options;
        this.clusterService = (ClientClusterServiceImpl)client.getClientClusterService();

//        try {
//            txnId = createTransaction();
//        } catch (IOException e) {
//            throw new HazelcastException("Could not create TransactionContext");
//        }
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
        return null;
    }

    public long getTimeoutMillis() {
        return 0;
    }



//    private String createTransaction() throws IOException {
//        return clusterService.sendAndReceiveFixedConnection(connection, new CreateTransactionRequest(options));
//    }
}
