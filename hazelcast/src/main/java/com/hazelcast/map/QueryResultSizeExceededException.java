/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.spi.properties.GroupProperty;

import static java.lang.String.format;

/**
 * This exception is thrown when a query exceeds a configurable result size limit.
 *
 * @see GroupProperty#QUERY_RESULT_SIZE_LIMIT
 */
public class QueryResultSizeExceededException extends HazelcastException {

    public QueryResultSizeExceededException(String message) {
        super(message);
    }

    public QueryResultSizeExceededException() {
        super("This exception has been thrown to prevent an OOME on this Hazelcast instance."
                + " An OOME might occur when a query collects large data sets from the whole cluster,"
                + " e.g. by calling IMap.values(), IMap.keySet() or IMap.entrySet()."
                + " See GroupProperty.QUERY_RESULT_SIZE_LIMIT for further details.");
    }

    public QueryResultSizeExceededException(int maxResultLimit, String optionalMessage) {
        super(format("This exception has been thrown to prevent an OOME on this Hazelcast instance."
                        + " An OOME might occur when a query collects large data sets from the whole cluster,"
                        + " e.g. by calling IMap.values(), IMap.keySet() or IMap.entrySet()."
                        + " See GroupProperty.QUERY_RESULT_SIZE_LIMIT for further details."
                        + " The configured query result size limit is %d items.%s",
                maxResultLimit, optionalMessage));
    }
}
