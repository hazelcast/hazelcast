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
package com.hazelcast.jet.sql.impl;

/**
 * Exception marking the query as completed even if the inbound edges are still producing values.
 *
 * Needed for streaming jobs, where {@link com.hazelcast.jet.core.Processor#complete} will never be called
 * if inbound edges are too fast. Should be always ignored on client side, it just means "no more data, but it's ok".
 */
class QueryEndException extends RuntimeException {

    QueryEndException() {
        // Use writableStackTrace = false, the exception is not created at a place where it's thrown,
        // it's better if it has no stack trace then.
        super("Done by reaching the end specified by the query", null, false, false);
    }
}
