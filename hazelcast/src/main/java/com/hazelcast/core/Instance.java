/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.core;

public interface Instance {

    enum InstanceType {
        QUEUE(1), MAP(2), SET(3), LIST(4), LOCK(5), TOPIC(6), MULTIMAP(7),
        ID_GENERATOR(8), ATOMIC_NUMBER(9), SEMAPHORE(10), COUNT_DOWN_LATCH(11);

        private final int typeId;

        InstanceType(int typeId) {
            this.typeId = typeId;
        }

        public static InstanceType valueOf(int typeId) {
            switch (typeId) {
                case 1:
                    return QUEUE;
                case 2:
                    return MAP;
                case 3:
                    return SET;
                case 4:
                    return LIST;
                case 5:
                    return LOCK;
                case 6:
                    return TOPIC;
                case 7:
                    return MULTIMAP;
                case 8:
                    return ID_GENERATOR;
                case 9:
                    return ATOMIC_NUMBER;
                case 10:
                    return SEMAPHORE;
                case 11:
                    return COUNT_DOWN_LATCH;
                default:
                    return MAP;
            }
        }

        public String prefix() {
            switch (this) {
                case MAP:
                    return Prefix.MAP;
                case QUEUE:
                    return Prefix.QUEUE;
                case LIST:
                    return Prefix.AS_LIST;
                case SET:
                    return Prefix.SET;
                case TOPIC:
                    return Prefix.TOPIC;
                case MULTIMAP:
                    return Prefix.AS_MULTIMAP;
                case ID_GENERATOR:
                    return Prefix.IDGEN;
                case COUNT_DOWN_LATCH:
                    return Prefix.COUNT_DOWN_LATCH;
                case SEMAPHORE:
                    return Prefix.SEMAPHORE;
                case ATOMIC_NUMBER:
                    return Prefix.ATOMIC_NUMBER;
                default:
                    return "";
            }
        }

        public int getTypeId() {
            return typeId;
        }

        public boolean isAtomicNumber() {
            return typeId == ATOMIC_NUMBER.typeId;
        }

        public boolean isCountDownLatch() {
            return typeId == COUNT_DOWN_LATCH.typeId;
        }

        public boolean isIdGenerator() {
            return typeId == ID_GENERATOR.typeId;
        }

        public boolean isList() {
            return typeId == LIST.typeId;
        }

        public boolean isLock() {
            return typeId == LOCK.typeId;
        }

        public boolean isMap() {
            return typeId == MAP.typeId;
        }

        public boolean isMultiMap() {
            return typeId == MULTIMAP.typeId;
        }

        public boolean isQueue() {
            return typeId == QUEUE.typeId;
        }

        public boolean isSemaphore() {
            return typeId == SEMAPHORE.typeId;
        }

        public boolean isSet() {
            return typeId == SET.typeId;
        }

        public boolean isTopic() {
            return typeId == TOPIC.typeId;
        }
    }

    /**
     * Returns instance type such as map, set, list, lock, topic, multimap, id generator
     *
     * @return instance type
     */
    InstanceType getInstanceType();

    /**
     * Destroys this instance cluster-wide.
     * Clears and releases all resources for this instance.
     */
    void destroy();

    /**
     * Returns the unique id for this instance.
     *
     * @return id the of this instance
     */
    Object getId();
}
