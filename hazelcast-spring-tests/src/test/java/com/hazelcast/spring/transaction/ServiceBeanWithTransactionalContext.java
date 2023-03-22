/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring.transaction;

import com.hazelcast.transaction.TransactionalTaskContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@Transactional
public class ServiceBeanWithTransactionalContext {

    TransactionalTaskContext transactionalContext;
    OtherServiceBeanWithTransactionalContext otherService;
    static final int TIMEOUT = 1;

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

    public void putUsingOtherBean_sameTransaction_withException(DummyObject object) {
        otherService.putWithException(object);
    }

    public void putUsingOtherBean_newTransaction(DummyObject object) {
        otherService.putInNewTransaction(object);
    }

    public void putUsingSameBean_thenOtherBeanThrowingException_sameTransaction(DummyObject object, DummyObject otherObject) {
        put(object);
        otherService.putWithException(otherObject);
    }

    public void putUsingOtherBean_thenSameBeanThrowingException_sameTransaction(DummyObject object, DummyObject otherObject) {
        otherService.put(otherObject);
        putWithException(object);
    }

    @Transactional
    public void putWithDelay(DummyObject object, int delay) {
        try {
            Thread.sleep(delay * 1000);
            put(object);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Transactional(timeout = TIMEOUT)
    public void putWithDelay_transactionTimeoutValue(DummyObject object, int delay) {
        try {
            Thread.sleep(delay * 1000);
            put(object);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
