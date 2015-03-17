package com.hazelcast.spi.impl.eventservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;

import java.io.IOException;

public class RegistrationOperation extends AbstractOperation {

    private Registration registration;
    private boolean response;

    public RegistrationOperation() {
    }

    public RegistrationOperation(Registration registration) {
        this.registration = registration;
    }

    @Override
    public void run() throws Exception {
        EventServiceImpl eventService = (EventServiceImpl) getNodeEngine().getEventService();
        response = eventService.handleRegistration(registration);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        registration.writeData(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        registration = new Registration();
        registration.readData(in);
    }
}
