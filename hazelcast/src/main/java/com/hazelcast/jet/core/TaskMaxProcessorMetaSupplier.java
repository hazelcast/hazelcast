/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.core;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * This class distributes specified number of processors evenly among cluster members
 */
public class TaskMaxProcessorMetaSupplier implements ProcessorMetaSupplier, IdentifiedDataSerializable {
    private int tasksMax;
    private ProcessorSupplier supplier;
    private boolean partitionedAddresses;
    private final List<Address> memberAddressList = new ArrayList<>();
    private final List<Integer> memberLocalParallelismList = new ArrayList<>();
    private int processorOrder;

    public void setTasksMax(int tasksMax) {
        this.tasksMax = tasksMax;
    }

    public void setSupplier(ProcessorSupplier supplier) {
        this.supplier = supplier;
    }

    private int getAndIncrementProcessorOrder(int delta) {
        int result = processorOrder;
        processorOrder = processorOrder + delta;
        return result;
    }

    @Override
    public void init(@Nonnull Context context) {
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
        return memberAddress -> {
            int indexOf = memberAddressList.indexOf(memberAddress);
            if (indexOf != -1) {
                memberAddressList.remove(indexOf);
                Integer localParallelismForMember = memberLocalParallelismList.remove(indexOf);
                return new TaskMaxProcessorSupplier(localParallelismForMember, supplier,
                        getAndIncrementProcessorOrder(localParallelismForMember));
            } else {
                return new ExpectNothingProcessorSupplier();
            }
        };
    }

    private void partitionTasks(List<Address> addresses) {
        int localParallelism = calculateAverageLocalParallelism(addresses);
        List<Address> shuffledAddresses = new ArrayList<>(addresses);
        Collections.shuffle(shuffledAddresses);

        int taskCounter = 0;

        while (taskCounter < tasksMax) {
            if (shuffledAddresses.size() == 1) {
                memberAddressList.add(shuffledAddresses.remove(0));
                memberLocalParallelismList.add(tasksMax - taskCounter);
                break;
            } else {
                memberAddressList.add(shuffledAddresses.remove(0));
                memberLocalParallelismList.add(localParallelism);
                taskCounter += localParallelism;
            }
        }
    }

    private int calculateAverageLocalParallelism(List<Address> addresses) {
        int averageLocalParallelism = tasksMax / addresses.size();
        if (averageLocalParallelism == 0) {
            averageLocalParallelism = 1;
        }
        return averageLocalParallelism;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(tasksMax);
        out.writeObject(supplier);
        out.writeInt(processorOrder);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        tasksMax = in.readInt();
        supplier = in.readObject();
        processorOrder = in.readInt();
    }

    @Override
    public int getFactoryId() {
        return JetDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JetDataSerializerHook.TASK_MAX_PROCESSOR_META_SUPPLIER;
    }
}
