package com.hazelcast.spring.transaction;

import com.hazelcast.transaction.TransactionalTaskContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OtherServiceBeanWithTransactionalContext {

    TransactionalTaskContext transactionalContext;


    public OtherServiceBeanWithTransactionalContext(TransactionalTaskContext transactionalContext) {
        this.transactionalContext = transactionalContext;
    }


    @Transactional
    public void put(DummyObject object) {
        transactionalContext.getMap("dummyObjectMap").put(object.getId(), object);
    }

    @Transactional
    public void putWithException(DummyObject object) {
        put(object);
        throw new RuntimeException("oops, let's rollback in " + this.getClass().getSimpleName() + "!");
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void putInNewTransaction(DummyObject object) {
        put(object);
    }
}
