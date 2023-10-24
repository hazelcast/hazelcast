/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc.join;

import com.hazelcast.jet.Traverser;

public class AutoCloseableTraverser<T> implements Traverser<T> {

    private final AutoCloseable autoCloseable;

    private final Traverser<T> traverser;

    public AutoCloseableTraverser(AutoCloseable autoCloseable, Traverser<T> traverser) {
        this.autoCloseable = autoCloseable;
        this.traverser = traverser;
    }

    @Override
    public T next() {
        return traverser.next();
    }

    @Override
    public void close() throws Exception {
        autoCloseable.close();
        Traverser.super.close();
    }
}
