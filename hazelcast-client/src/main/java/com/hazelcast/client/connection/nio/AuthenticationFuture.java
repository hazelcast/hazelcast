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

package com.hazelcast.client.connection.nio;

import com.hazelcast.nio.Connection;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class AuthenticationFuture {

    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private ClientConnection connection;
    private Throwable throwable;

    void onSuccess(ClientConnection connection) {
        this.connection = connection;
        countDownLatch.countDown();
    }

    void onFailure(Throwable throwable) {
        this.throwable = throwable;
        countDownLatch.countDown();
    }

    Connection get(int timeout) throws Throwable {
        if (!countDownLatch.await(timeout, TimeUnit.MILLISECONDS)) {
            throw new TimeoutException("Authentication response did not come back in " + timeout + " millis");
        }
        if (connection != null) {
            return connection;
        }
        assert throwable != null;
        throw throwable;
    }
}
