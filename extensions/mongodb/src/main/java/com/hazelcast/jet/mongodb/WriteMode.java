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
 *
 * @since 5.3
 */
public enum WriteMode {
    /**
     * Items will be inserted to MongoDB; in case of ID clash, error will be thrown.
     */
    INSERT_ONLY,
    /**
     * Performs update. Entity with the given {@code _id} must already exist, otherwise an error will be thrown.
     */
    UPDATE_ONLY,
    /**
     * Performs update. If the entity with the given {@code _id} does not exist, it will be created.
     */
    UPSERT,
    /**
     * Performs replace. If the entity with the given {@code _id} does not exist, it will be created (if it was not
     * overridden by the user).
     * <p>
     * Replace is different from UPSERT in handling of missing fields. Replace will set them to null,
     * while upsert won't affect such fields in the entity.
     */
    REPLACE;
}
