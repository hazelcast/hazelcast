package com.hazelcast.transaction.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.transaction.TransactionOptions.TransactionType.ONE_PHASE;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests some basic behavior that doesn't rely too much on the type of transaction
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TransactionImplTest extends HazelcastTestSupport {

    private InternalOperationService operationService;
    private ILogger logger;
    private TransactionManagerServiceImpl txManagerService;
    private NodeEngine nodeEngine;
    private TransactionOptions options;

    @Before
    public void setup() {
        HazelcastInstance hz = createHazelcastInstance();
        operationService = getOperationService(hz);
        logger = mock(ILogger.class);

        txManagerService = mock(TransactionManagerServiceImpl.class);

        nodeEngine = mock(NodeEngine.class);
        when(nodeEngine.getOperationService()).thenReturn(operationService);
        when(nodeEngine.getLocalMember()).thenReturn(new MemberImpl());
        when(nodeEngine.getLogger(TransactionImpl.class)).thenReturn(logger);
        options = new TransactionOptions().setTransactionType(ONE_PHASE);
    }

    @Test
    public void getTimeoutMillis() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        assertEquals(options.getTimeoutMillis(), tx.getTimeoutMillis());
    }

    @Test
    public void testToString() throws Exception {
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, "dummy-uuid");
        assertEquals(format("Transaction{txnId='%s', state=%s, txType=%s, timeoutMillis=%s}",
                tx.getTxnId(), tx.getState(), options.getTransactionType(), options.getTimeoutMillis()), tx.toString());
    }

    @Test
    public void getOwnerUUID() throws Exception {
        String ownerUUID = "dummy-uuid";
        TransactionImpl tx = new TransactionImpl(txManagerService, nodeEngine, options, ownerUUID);
        assertEquals(ownerUUID, tx.getOwnerUuid());
    }

}
