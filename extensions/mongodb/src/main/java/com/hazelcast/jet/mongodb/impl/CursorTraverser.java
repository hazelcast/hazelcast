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
package com.hazelcast.jet.mongodb.impl;

import com.hazelcast.jet.Traverser;
import com.mongodb.client.MongoCursor;

/**
 * A traverser that will emit {@link MongoCursor#tryNext()} value
 * or {@link EmptyItem#INSTANCE} in case of first null encountered.
 */
public class CursorTraverser implements Traverser<Object> {
    private final MongoCursor<?> cursor;
    private volatile boolean ended;

    public CursorTraverser(MongoCursor<?> cursor) {
        this.cursor = cursor;
        this.ended = false;
    }

    @Override
    public Object next() {
        Object next = cursor.tryNext();
        if (next == null) {
            boolean alreadyEmittedLast = ended;
            ended = true;
            return alreadyEmittedLast ? null : EmptyItem.INSTANCE;
        }
        return next;
    }

    enum EmptyItem {
        INSTANCE
    }
}
