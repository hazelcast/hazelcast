/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

public enum ClusterOperation {
    NONE(-1),
    RESPONSE(1),
    HEARTBEAT(2),
    REMOTELY_PROCESS(3),
    REMOTELY_PROCESS_AND_RESPOND(4),
    REMOTELY_CALLABLE_BOOLEAN(5),
    REMOTELY_CALLABLE_OBJECT(6),
    ADD_LISTENER(51),
    REMOVE_LISTENER(52),
    EVENT(53),
    REMOTELY_EXECUTE(81),
    STREAM(82),
    BLOCKING_QUEUE_POLL(101),
    BLOCKING_QUEUE_OFFER(102),
    BLOCKING_QUEUE_ADD_BLOCK(103),
    BLOCKING_QUEUE_REMOVE_BLOCK(104),
    BLOCKING_QUEUE_FULL_BLOCK(105),
    BLOCKING_QUEUE_BACKUP_ADD(107),
    BLOCKING_QUEUE_BACKUP_REMOVE(108),
    BLOCKING_QUEUE_SIZE(109),
    BLOCKING_QUEUE_PEEK(110),
    BLOCKING_QUEUE_READ(111),
    BLOCKING_QUEUE_REMOVE(112),
    BLOCKING_QUEUE_TXN_BACKUP_POLL(113),
    BLOCKING_QUEUE_TXN_COMMIT(114),
    BLOCKING_QUEUE_PUBLISH(115),
    BLOCKING_QUEUE_ADD_TOPIC_LISTENER(116),
    CONCURRENT_MAP_PUT(201),
    CONCURRENT_MAP_GET(202),
    CONCURRENT_MAP_REMOVE(203),
    CONCURRENT_MAP_REMOVE_ITEM(204),
    CONCURRENT_MAP_GET_MAP_ENTRY(205),
    CONCURRENT_MAP_BLOCK_INFO(206),
    CONCURRENT_MAP_SIZE(207),
    CONCURRENT_MAP_CONTAINS(208),
    CONCURRENT_MAP_ITERATE_ENTRIES(209),
    CONCURRENT_MAP_ITERATE_KEYS(210),
    CONCURRENT_MAP_ITERATE_VALUES(211),
    CONCURRENT_MAP_LOCK(212),
    CONCURRENT_MAP_UNLOCK(213),
    CONCURRENT_MAP_BLOCKS(214),
    CONCURRENT_MAP_MIGRATION_COMPLETE(216),
    CONCURRENT_MAP_PUT_IF_ABSENT(219),
    CONCURRENT_MAP_REMOVE_IF_SAME(220),
    CONCURRENT_MAP_REPLACE_IF_NOT_NULL(221),
    CONCURRENT_MAP_REPLACE_IF_SAME(222),
    CONCURRENT_MAP_LOCK_RETURN_OLD(223),
    CONCURRENT_MAP_READ(224),
    CONCURRENT_MAP_ADD_TO_LIST(225),
    CONCURRENT_MAP_ADD_TO_SET(226),
    CONCURRENT_MAP_MIGRATE_RECORD(227),
    CONCURRENT_MAP_PUT_MULTI(228),
    CONCURRENT_MAP_REMOVE_MULTI(229),
    CONCURRENT_MAP_VALUE_COUNT(230),
    CONCURRENT_MAP_BACKUP_PUT(231),
    CONCURRENT_MAP_BACKUP_REMOVE(233),
    CONCURRENT_MAP_BACKUP_REMOVE_MULTI(234),
    CONCURRENT_MAP_BACKUP_LOCK(235),
    CONCURRENT_MAP_BACKUP_ADD(236),
    CONCURRENT_MAP_EVICT(237),
    ;

    private int value;

    ClusterOperation(int op){
        value = op;
    }

    public int getValue(){
        return value;
    }

    public static ClusterOperation create(int operation){
        switch(operation){
            case -1: return NONE;
            case 1: return RESPONSE;
            case 2: return HEARTBEAT;
            case 3: return REMOTELY_PROCESS;
            case 4: return REMOTELY_PROCESS_AND_RESPOND;
            case 5: return REMOTELY_CALLABLE_BOOLEAN;
            case 6: return REMOTELY_CALLABLE_OBJECT;
            case 51: return ADD_LISTENER;
            case 52: return REMOVE_LISTENER;
            case 53: return EVENT;
            case 81: return REMOTELY_EXECUTE;
            case 82: return STREAM;
            case 101: return BLOCKING_QUEUE_POLL;
            case 102: return BLOCKING_QUEUE_OFFER;
            case 103: return BLOCKING_QUEUE_ADD_BLOCK;
            case 104: return BLOCKING_QUEUE_REMOVE_BLOCK;
            case 105: return BLOCKING_QUEUE_FULL_BLOCK;
            case 107: return BLOCKING_QUEUE_BACKUP_ADD;
            case 108: return BLOCKING_QUEUE_BACKUP_REMOVE;
            case 109: return BLOCKING_QUEUE_SIZE;
            case 110: return BLOCKING_QUEUE_PEEK;
            case 111: return BLOCKING_QUEUE_READ;
            case 112: return BLOCKING_QUEUE_REMOVE;
            case 113: return BLOCKING_QUEUE_TXN_BACKUP_POLL;
            case 114: return BLOCKING_QUEUE_TXN_COMMIT;
            case 115: return BLOCKING_QUEUE_PUBLISH;
            case 116: return BLOCKING_QUEUE_ADD_TOPIC_LISTENER;
            case 201: return CONCURRENT_MAP_PUT;
            case 202: return CONCURRENT_MAP_GET;
            case 203: return CONCURRENT_MAP_REMOVE;
            case 204: return CONCURRENT_MAP_REMOVE_ITEM;
            case 205: return CONCURRENT_MAP_GET_MAP_ENTRY;
            case 206: return CONCURRENT_MAP_BLOCK_INFO;
            case 207: return CONCURRENT_MAP_SIZE;
            case 208: return CONCURRENT_MAP_CONTAINS;
            case 209: return CONCURRENT_MAP_ITERATE_ENTRIES;
            case 210: return CONCURRENT_MAP_ITERATE_KEYS;
            case 211: return CONCURRENT_MAP_ITERATE_VALUES;
            case 212: return CONCURRENT_MAP_LOCK;
            case 213: return CONCURRENT_MAP_UNLOCK;
            case 214: return CONCURRENT_MAP_BLOCKS;
            case 216: return CONCURRENT_MAP_MIGRATION_COMPLETE;
            case 219: return CONCURRENT_MAP_PUT_IF_ABSENT;
            case 220: return CONCURRENT_MAP_REMOVE_IF_SAME;
            case 221: return CONCURRENT_MAP_REPLACE_IF_NOT_NULL;
            case 222: return CONCURRENT_MAP_REPLACE_IF_SAME;
            case 223: return CONCURRENT_MAP_LOCK_RETURN_OLD;
            case 224: return CONCURRENT_MAP_READ;
            case 225: return CONCURRENT_MAP_ADD_TO_LIST;
            case 226: return CONCURRENT_MAP_ADD_TO_SET;
            case 227: return CONCURRENT_MAP_MIGRATE_RECORD;
            case 228: return CONCURRENT_MAP_PUT_MULTI;
            case 229: return CONCURRENT_MAP_REMOVE_MULTI;
            case 230: return CONCURRENT_MAP_VALUE_COUNT;
            case 231: return CONCURRENT_MAP_BACKUP_PUT;
            case 233: return CONCURRENT_MAP_BACKUP_REMOVE;
            case 234: return CONCURRENT_MAP_BACKUP_REMOVE_MULTI;
            case 235: return CONCURRENT_MAP_BACKUP_LOCK;
            case 236: return CONCURRENT_MAP_BACKUP_ADD;
            case 237: return CONCURRENT_MAP_EVICT;
            default: return null;
        }
    }
}
