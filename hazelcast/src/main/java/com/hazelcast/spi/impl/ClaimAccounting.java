package com.hazelcast.spi.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.nio.ConnectionManager;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Responsible for dealing with back-pressure claim requests.
 */
public class ClaimAccounting implements ConnectionListener {
    public final int totalCapacity;
    public final int maximumClaimSize;
    public final int minimumClaimSize;

    private final AtomicInteger bookedCapacity = new AtomicInteger();
    private final ConcurrentMap<Connection, Integer> bookedCapacityPerMember = new ConcurrentHashMap<Connection, Integer>();

    private final InternalOperationService internalOperationService;
    private final Node node;
    private final ILogger logger;

    public ClaimAccounting(InternalOperationService internalOperationService, Node node) {
        this.internalOperationService = internalOperationService;
        this.node = node;
        this.logger = node.getLogger(ClaimAccounting.class);

        this.totalCapacity = node.getGroupProperties().BACKPRESSURE_TOTAL_CAPACITY.getInteger();
        this.maximumClaimSize = node.getGroupProperties().BACKPRESSURE_MAX_CLAIM_SIZE.getInteger();
        this.minimumClaimSize = node.getGroupProperties().BACKPRESSURE_MIN_CLAIM_SIZE.getInteger();

    }

    public int claimSlots(Connection connection) {
        ConnectionManager connectionManager = node.getConnectionManager();

        int newClaim;
        for ( ;; ) {
            int bookedCapacityBefore = bookedCapacity.get();
            Integer myClaimsBefore = bookedCapacityPerMember.get(connection);
            myClaimsBefore = myClaimsBefore != null ? myClaimsBefore : 0;

            int noOfScheduledOperations = internalOperationService.getNoOfScheduledOperations();
            int bookedCapacityWithoutMe = bookedCapacityBefore - myClaimsBefore;
            int remainingCapacity = totalCapacity - noOfScheduledOperations - bookedCapacityWithoutMe;
            int activeConnectionCount = connectionManager.getActiveConnectionCount();
            newClaim = remainingCapacity / activeConnectionCount;
            if (newClaim >= minimumClaimSize) {
                newClaim = Math.min(maximumClaimSize, newClaim);
            } else {
                newClaim = 0;
            }
            if (logger.isFinestEnabled()) {
                logger.finest("Number of scheduled operations: " + noOfScheduledOperations
                        + ", Booked capacity: " + bookedCapacityWithoutMe + ", Active connection count :"
                        + activeConnectionCount + ", new claim for connection " + connection + " is: " + newClaim);
            }

            int reservedCapacityAfter = bookedCapacityWithoutMe + newClaim;
            if (bookedCapacity.compareAndSet(bookedCapacityBefore, reservedCapacityAfter)) {
                break;
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("CAS has failed. I have to compute claim size for connection " + connection
                        + " once again.");
                }
            }
        }
        bookedCapacityPerMember.put(connection, newClaim);
        return newClaim;
    }

    @Override
    public void connectionAdded(Connection connection) {

    }

    @Override
    public void connectionRemoved(Connection connection) {
        Integer claim = bookedCapacityPerMember.get(connection);
        if (claim != null) {
            bookedCapacity.getAndAdd(-claim);
            bookedCapacityPerMember.remove(connection);
        }
    }
}
