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
package com.hazelcast.jet.mongodb;

/**
 * Defines allowed operations.
 */
public enum WriteMode {
    /**
     * All items will be inserted to mongo; in case of id clash, error will be thrown.
     */
    INSERT_ONLY,
    /**
     * Performs update. Entity with given {@code _id} should already exist, otherwise error will be thrown.
     */
    UPDATE_ONLY,
    /**
     * Performs update. If entity with given {@code _id} does not exist, it will be created.
     */
    UPSERT,
    /**
     * Performs replace. If entity with given {@code _id} does not exist, it will be created (if it was not
     * overridden by the user).
     *
     * Replace is different from UPSERT in regard to missing fields. Replace will set such to null,
     * while update won't affect such fields in collection.
     */
    REPLACE
}
