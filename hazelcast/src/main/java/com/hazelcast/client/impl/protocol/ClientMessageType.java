/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol;

/**
 * Client Message type is the unique id defines the type of message. Each type is handled on a specific handler by this id
 */
public enum ClientMessageType {

    /**
     * Default Authentication with user-name and password
     */
    AUTHENTICATION_DEFAULT_REQUEST(1),

    /**
     * Custom Authentication with custom credentials impl
     */
    AUTHENTICATION_CUSTOM_REQUEST(2),

    /**
     * Exception
     */
    EXCEPTION(3),

    /**
     * Result wrapper message type
     */
    RESULT(4),

    /**
     * Event registration id
     */
    ADD_LISTENER_RESULT(5),

    ADD_ENTRY_LISTENER_EVENT(6),

    REGISTER_MEMBERSHIP_LISTENER_REQUEST(8),

    REGISTER_MEMBERSHIP_LISTENER_EVENT(9),

    CREATE_PROXY_REQUEST(11),

    GET_PARTITIONS_REQUEST(12),

    GET_PARTITIONS_RESULT(13),

    AUTHENTICATION_RESULT(14),

    BOOLEAN_RESULT(15),

    INTEGER_RESULT(16),

    DATA_LIST_RESULT(17),

    DATA_ENTRY_LIST_RESULT(18),

    MEMBER_RESULT(19),

    MEMBER_LIST_RESULT(20),

    MEMBER_ATTRIBUTE_RESULT(21),

    VOID_RESULT(22),

    ENTRY_VIEW(23),

    DESTROY_PROXY_REQUEST(24),

    ITEM_EVENT(25),

    TOPIC_EVENT(26),

    LONG_RESULT(27),

    PARTITION_LOST_EVENT(28),

    REMOVE_ALL_LISTENERS(29),

    ADD_PARTITION_LOST_LISTENER(30),

    REMOVE_PARTITION_LOST_LISTENER(31),

    GET_DISTRIBUTED_OBJECT(32),

    ADD_DISTRIBUTED_OBJECT_LISTENER(33),

    REMOVE_DISTRIBUTED_OBJECT_LISTENER(34),

    TRANSACTION_RECOVER_ALL(35),

    TRANSACTION_RECOVER(36),

    TRANSACTION_CREATE_RESULT(37),

    TRANSACTION_CREATE(38),

    TRANSACTION_PREPARE(39),

    TRANSACTION_COMMIT(40),

    TRANSACTION_ROLLBACK(41),

    PING(42),

    MAP_INT_DATA_RESULT(90);

    private final int id;

    ClientMessageType(int messageType) {
        this.id = messageType;
    }

    public int id() {
        return id;
    }

}
