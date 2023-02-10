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
package com.hazelcast.jet.sql.impl.connector.mongodb;

final class Options {

    static final String DATA_LINK_REF_OPTION = "dataLinkRef";
    static final String CONNECTION_STRING_OPTION = "connectionString";
    static final String DATABASE_NAME_OPTION = "database";
    static final String COLLECTION_NAME_OPTION = "collection";
    static final String START_AT_OPTION = "startAt";

    private Options() {
    }

}
