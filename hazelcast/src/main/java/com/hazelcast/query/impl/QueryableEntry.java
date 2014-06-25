/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.query.impl;

import java.util.Map;

/**
 *  This interface contains methods related to Queryable Entry which means searched an indexed by sql query or predicate .
 */
public interface QueryableEntry extends Map.Entry, QueryResultEntry {

    Object getValue();

    Object getKey();

    Comparable getAttribute(String attributeName) throws QueryException;

    AttributeType getAttributeType(String attributeName);
}
