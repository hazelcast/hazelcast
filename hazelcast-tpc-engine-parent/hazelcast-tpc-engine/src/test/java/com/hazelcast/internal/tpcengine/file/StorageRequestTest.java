/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpcengine.file;

import org.junit.Test;

import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_CLOSE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FALLOCATE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FDATASYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_FSYNC;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_NOP;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_OPEN;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_READ;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.STR_REQ_OP_WRITE;
import static com.hazelcast.internal.tpcengine.file.StorageRequest.storageReqOpcodeToString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Bogus test to up the coverage for this class.
 */
public class StorageRequestTest {

    @Test
    public void test_storageReqOpcodeToString() {
        assertEquals("STR_REQ_OP_NOP", storageReqOpcodeToString(STR_REQ_OP_NOP));
        assertEquals("STR_REQ_OP_READ", storageReqOpcodeToString(STR_REQ_OP_READ));
        assertEquals("STR_REQ_OP_WRITE", storageReqOpcodeToString(STR_REQ_OP_WRITE));
        assertEquals("STR_REQ_OP_FSYNC", storageReqOpcodeToString(STR_REQ_OP_FSYNC));
        assertEquals("STR_REQ_OP_FDATASYNC", storageReqOpcodeToString(STR_REQ_OP_FDATASYNC));
        assertEquals("STR_REQ_OP_OPEN", storageReqOpcodeToString(STR_REQ_OP_OPEN));
        assertEquals("STR_REQ_OP_CLOSE", storageReqOpcodeToString(STR_REQ_OP_CLOSE));
        assertEquals("STR_REQ_OP_FALLOCATE", storageReqOpcodeToString(STR_REQ_OP_FALLOCATE));
        assertEquals("unknown-opcode(-1)", storageReqOpcodeToString(-1));
    }

    @Test
    public void test_toString() {
        StorageRequest req = new StorageRequest();
        assertNotNull(req.toString());
    }
}
