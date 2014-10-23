package com.hazelcast.spi.impl;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ClaimAccountingTest extends HazelcastTestSupport {

    private InternalOperationService internalOperationService;
    private ConnectionManager connectionManager;

    @Before
    public void setUp() {
        internalOperationService = mock(InternalOperationService.class);
        connectionManager = mock(ConnectionManager.class);
    }
//
//    @Test
//    public void testClaimSlots_maximumNumberOfSlotsCantExceedThreshold() throws Exception {
//        when(internalOperationService.getNoOfScheduledOperations()).thenReturn(10);
//        Connection mockConnection = mock(Connection.class);
//        ClaimAccounting accounting = new ClaimAccounting(internalOperationService, connectionManager);
//        int slots = accounting.claimSlots(mockConnection);
//        assertLesserOrEquals(ClaimAccounting.maximumClaimSize, slots);
//    }





}