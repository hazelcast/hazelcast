/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.concurrent.flakeidgen;

import com.hazelcast.core.FlakeIdGenerator;
import com.hazelcast.core.IdBatch;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.Clock;

import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.Preconditions.checkPositive;

public class FlakeIdGeneratorProxy
        extends AbstractDistributedObject<FlakeIdGeneratorService>
        implements FlakeIdGenerator {

    static final int BITS_TIMESTAMP = 42;

    /**
     * 1.1.2017 0:00 GMT will be MIN_VALUE in the 42-bit signed integer.
     * <p>
     * {@code 1483228800000} is the value {@code System.currentTimeMillis()} would return on
     * 1.1.2017 0:00 GMT.
     */
    @SuppressWarnings("checkstyle:magicnumber")
    static final long EPOCH_START = 1483228800000L + (1L << (BITS_TIMESTAMP - 1));

    private static final int BITS_SEQUENCE = 6;
    private static final int BITS_NODE_ID = 16;

    private final int nodeId;
    private final String name;
    private final ILogger logger;

    /**
     * The highest timestamp|seq value returned so far. The value is not shifted to most significant bits.
     */
    private final AtomicLong generatedValue = new AtomicLong(Long.MIN_VALUE);

    FlakeIdGeneratorProxy(String name, int nodeId, NodeEngine nodeEngine, FlakeIdGeneratorService service) {
        super(nodeEngine, service);
        this.name = name;
        this.logger = nodeEngine.getLogger(getClass());

        // If nodeId is out of range, we'll allow to create the proxy, but we'll refuse to generate IDs.
        // This is to provide functionality when the node ID overflows until the cluster is restarted and
        // node IDs are assigned from 0 again.
        // It won't be possible to generate IDs through member HZ instances, however clients ignore
        // members which threw FlakeIdNodeIdOverflowException, so they'll work as long as there is
        // at least one member with node ID in the cluster.
        if ((nodeId & -1 << BITS_NODE_ID) != 0) {
            this.nodeId = -1;
            logger.severe("Node ID is out of range (" + nodeId + "), this member won't be able to generate IDs. "
                    + "Cluster restart is recommended.");
        } else {
            this.nodeId = nodeId;
        }
    }

    @Override
    public long newId() {
        return newIdBase(Clock.currentTimeMillis(), 1);
    }

    @Override
    public IdBatch newIdBatch(int batchSize) {
        long base = newIdBase(Clock.currentTimeMillis(), batchSize);
        return new IdBatch(base, 1 << BITS_NODE_ID, batchSize);
    }

    /**
     * The layout of the ID is as follows (starting from most significant bits):<ul>
     *     <li>42 bits timestamp
     *     <li>6 bits sequence
     *     <li>16 bits node ID
     * </ul>
     *
     * This order is important: timestamp must be first to keep IDs ordered. Sequence must be second for
     * implementation reasons (it's included in {@link #generatedValue}). Node is just an appendix to make
     * IDs unique.
     *
     * @param now Current time (currentTimeMillis() normally or other value in tests)
     * @return First ID for the batch.
     */
    long newIdBase(long now, int batchSize) {
        checkPositive(batchSize, "batchSize");
        if (nodeId < 0) {
            throw new FlakeIdNodeIdOverflowException("NodeID overflow, this member cannot generate IDs");
        }
        now -= EPOCH_START;
        assert now >= -(1L << BITS_TIMESTAMP - 1) && now < (1L << BITS_TIMESTAMP - 1)  : "Current time out of allowed range";
        now <<= BITS_SEQUENCE;
        long oldGeneratedValue;
        long base;
        do {
            oldGeneratedValue = generatedValue.get();
            base = Math.max(now, oldGeneratedValue);
        } while (!generatedValue.compareAndSet(oldGeneratedValue, base + batchSize));
        return base << BITS_NODE_ID | nodeId;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getServiceName() {
        return FlakeIdGeneratorService.SERVICE_NAME;
    }
}
