package com.hazelcast.spring.transaction;

import com.hazelcast.transaction.TransactionalTaskContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ServiceBeanWithTransactionalContext {

    TransactionalTaskContext transactionalContext;
    OtherServiceBeanWithTransactionalContext otherService;

    public ServiceBeanWithTransactionalContext(TransactionalTaskContext transactionalContext,
                                               OtherServiceBeanWithTransactionalContext otherService) {
        this.transactionalContext = transactionalContext;
        this.otherService = otherService;
    }


    public void put(DummyObject object) {
        transactionalContext.getMap("dummyObjectMap").put(object.getId(), object);
    }

    public void putWithException(DummyObject object) {
        put(object);
        throw new RuntimeException("oops, let's rollback!");
    }

    public void putUsingOtherBean_sameTransaction(DummyObject object) {
        otherService.put(object);
    }

    public void putUsingOtherBean_newTransaction(DummyObject object) {
        otherService.putInNewTransaction(object);
    }
}
