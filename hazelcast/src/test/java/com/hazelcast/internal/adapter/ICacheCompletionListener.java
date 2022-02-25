/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.adapter;

import com.hazelcast.test.HazelcastTestSupport;

import javax.cache.integration.CompletionListener;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public class ICacheCompletionListener implements CompletionListener {

    private final CountDownLatch completed = new CountDownLatch(1);

    private volatile Exception exception;

    @Override
    public void onCompletion() {
        completed.countDown();
    }

    @Override
    public void onException(Exception e) {
        exception = e;
        completed.countDown();
    }

    public void await() {
        HazelcastTestSupport.assertOpenEventually(completed);
        if (exception != null) {
            throw rethrow(exception);
        }
    }
}
