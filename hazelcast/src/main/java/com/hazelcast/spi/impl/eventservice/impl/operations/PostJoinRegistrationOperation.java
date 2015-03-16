package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class PostJoinRegistrationOperation extends AbstractOperation {

    private Collection<Registration> registrations;

    public PostJoinRegistrationOperation() {
    }

    public PostJoinRegistrationOperation(Collection<Registration> registrations) {
        this.registrations = registrations;
    }

    @Override
    public void run() throws Exception {
        if (registrations == null || registrations.size() <= 0) {
            return;
        }

        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        EventServiceImpl eventService = (EventServiceImpl) nodeEngine.getEventService();
        for (Registration reg : registrations) {
            eventService.handleRegistration(reg);
        }
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        int len = registrations != null ? registrations.size() : 0;
        out.writeInt(len);
        if (len > 0) {
            for (Registration reg : registrations) {
                reg.writeData(out);
            }
        }
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int len = in.readInt();
        if (len > 0) {
            registrations = new ArrayList<Registration>(len);
            for (int i = 0; i < len; i++) {
                Registration reg = new Registration();
                registrations.add(reg);
                reg.readData(in);
            }
        }
    }
}
