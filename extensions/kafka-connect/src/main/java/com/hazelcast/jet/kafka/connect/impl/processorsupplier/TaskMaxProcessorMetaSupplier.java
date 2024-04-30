/*
 * Copyright 2024 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.hazelcast.jet.kafka.connect.impl.processorsupplier;

import com.hazelcast.cluster.Address;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serial;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * This class distributes specified number of processors evenly among cluster members
 * Client or member sends this class to job coordinator in serialized form
 */
public class TaskMaxProcessorMetaSupplier implements ProcessorMetaSupplier, DataSerializable {

    @Serial
    private static final long serialVersionUID = 1L;

    private int tasksMax;
    private ReadKafkaConnectProcessorSupplier supplier;
    private transient boolean partitionedAddresses;
    private final Map<Address, Integer> startingProcessorOrderMap = new HashMap<>();
    private transient int localParallelism;
    private int lastInitiallyActiveProcessorOrder;

    public void setTasksMax(int tasksMax) {
        this.tasksMax = tasksMax;
    }

    public void setSupplier(ReadKafkaConnectProcessorSupplier supplier) {
        this.supplier = supplier;
    }

    public boolean isPartitionedAddresses() {
        return partitionedAddresses;
    }

    @Override
    public void init(@Nonnull Context context) {
        localParallelism = context.localParallelism();
        // Determine whether the execution plan can accommodate the tasksMax during the planning phase
        int totalParallelism = context.totalParallelism();
        if (totalParallelism < tasksMax) {
            throw new IllegalArgumentException("The requested parallelism of " + tasksMax + " is greater than " +
                                               "the available parallelism of " + totalParallelism +
                                               " for Kafka Connect vertices. " +
                                               "Please call setLocalParallelism(" + tasksMax + ") " +
                                               "for Kafka Connect Source"
            );
        }
    }

    @Nonnull
    @Override
    public Function<? super Address, ? extends ProcessorSupplier> get(@Nonnull List<Address> addresses) {
        if (!partitionedAddresses) {
            partitionedAddresses = true;
            partitionTasks(addresses);
        }
        final int lastActive = this.lastInitiallyActiveProcessorOrder;
        return memberAddress -> {
            var startingProcessorOrder = startingProcessorOrderMap.get(memberAddress);
            if (startingProcessorOrder != null) {
                return new TaskMaxProcessorSupplier(startingProcessorOrder, lastActive, supplier);
            } else {
                return new ExpectNothingProcessorSupplier();
            }
        };
    }

    /**
     * Algorithm:
     * We get few addresses, then we iterate and for each we set Kafka Connect's
     * {@code local parallelism = local parallelism of member}. If it's the last address on the list, we set
     * KC {@code local parallelism as tasksMax - already created task count}.
     *  <p>
     * All the addresses without an assigned value will have KC local parallelism = 0 and will never become active -
     * we will use {@link com.hazelcast.jet.core.ProcessorMetaSupplier.ExpectNothingProcessorSupplier} for them.
     */
    private void partitionTasks(List<Address> addresses) {
        List<Address> copiedAddresses = new ArrayList<>(addresses);
        Collections.shuffle(copiedAddresses);
        int taskCounter = 0;

        int processorOrder = 0;
        boolean allMembersAssigned = false;
        while (taskCounter < tasksMax && !allMembersAssigned) {
            Address address = copiedAddresses.remove(0);
            int parallelismForMember;
            // fill to the tasksMax value if we are at last member
            if (copiedAddresses.isEmpty()) {
                parallelismForMember = tasksMax - taskCounter;
                allMembersAssigned = true;
            } else {
                parallelismForMember = localParallelism;
            }
            taskCounter += parallelismForMember;
            startingProcessorOrderMap.put(address, processorOrder);
            processorOrder += localParallelism;
        }
        lastInitiallyActiveProcessorOrder = taskCounter - 1;
    }

    Map<Address, Integer> getStartingProcessorOrderMap() {
        return startingProcessorOrderMap;
    }

    void setLocalParallelism(int localParallelism) {
        this.localParallelism = localParallelism;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(tasksMax);
        out.writeObject(supplier);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        tasksMax = in.readInt();
        supplier = in.readObject();
    }
}
