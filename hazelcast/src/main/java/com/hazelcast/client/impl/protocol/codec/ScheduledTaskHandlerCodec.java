package com.hazelcast.client.impl.protocol.codec;

import com.hazelcast.annotation.Codec;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ParameterUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Bits;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskHandlerImpl;

@Codec(ScheduledTaskHandler.class)
public final class ScheduledTaskHandlerCodec {

    private ScheduledTaskHandlerCodec() {
    }

    public static ScheduledTaskHandler decode(ClientMessage clientMessage) {
        String schedulerName = clientMessage.getStringUtf8();
        String taskName = clientMessage.getStringUtf8();
        boolean isToAddress = clientMessage.getBoolean();
        if (isToAddress) {
            Address address = AddressCodec.decode(clientMessage);
            return ScheduledTaskHandlerImpl.of(address, schedulerName, taskName);
        } else {
            int partitionId = clientMessage.getInt();
            return ScheduledTaskHandlerImpl.of(partitionId, schedulerName, taskName);
        }
    }

    public static void encode(ScheduledTaskHandler scheduledTaskHandler, ClientMessage clientMessage) {
        clientMessage.set(scheduledTaskHandler.getSchedulerName());
        clientMessage.set(scheduledTaskHandler.getTaskName());
        Address address = scheduledTaskHandler.getAddress();
        boolean isToAddress = address != null;
        clientMessage.set(isToAddress);
        if (isToAddress) {
            AddressCodec.encode(address, clientMessage);
        } else {
            clientMessage.set(scheduledTaskHandler.getPartitionId());
        }
    }

    public static int calculateDataSize(ScheduledTaskHandler scheduledTaskHandler) {
        int dataSize = ParameterUtil.calculateDataSize(scheduledTaskHandler.getSchedulerName());
        dataSize += ParameterUtil.calculateDataSize(scheduledTaskHandler.getTaskName());
        // is to address field
        dataSize += Bits.BOOLEAN_SIZE_IN_BYTES;
        Address address = scheduledTaskHandler.getAddress();
        if (address != null) {
            dataSize += AddressCodec.calculateDataSize(address);
        } else {
            dataSize += Bits.INT_SIZE_IN_BYTES;
        }
        return dataSize;
    }
}
