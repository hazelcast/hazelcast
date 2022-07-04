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

package com.hazelcast.client.impl.protocol.codec.custom;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.Generated;
import com.hazelcast.client.impl.protocol.codec.builtin.*;

import static com.hazelcast.client.impl.protocol.codec.builtin.CodecUtil.fastForwardToEndFrame;
import static com.hazelcast.client.impl.protocol.ClientMessage.*;
import static com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec.*;

@Generated("7b6e19309b9215212589eaa24ad14c12")
public final class MigrationStateCodec {
    private static final int START_TIME_FIELD_OFFSET = 0;
    private static final int PLANNED_MIGRATIONS_FIELD_OFFSET = START_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;
    private static final int COMPLETED_MIGRATIONS_FIELD_OFFSET = PLANNED_MIGRATIONS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int TOTAL_ELAPSED_TIME_FIELD_OFFSET = COMPLETED_MIGRATIONS_FIELD_OFFSET + INT_SIZE_IN_BYTES;
    private static final int INITIAL_FRAME_SIZE = TOTAL_ELAPSED_TIME_FIELD_OFFSET + LONG_SIZE_IN_BYTES;

    private MigrationStateCodec() {
    }

    public static void encode(ClientMessage clientMessage, com.hazelcast.partition.MigrationState migrationState) {
        clientMessage.add(BEGIN_FRAME.copy());

        ClientMessage.Frame initialFrame = new ClientMessage.Frame(new byte[INITIAL_FRAME_SIZE]);
        encodeLong(initialFrame.content, START_TIME_FIELD_OFFSET, migrationState.getStartTime());
        encodeInt(initialFrame.content, PLANNED_MIGRATIONS_FIELD_OFFSET, migrationState.getPlannedMigrations());
        encodeInt(initialFrame.content, COMPLETED_MIGRATIONS_FIELD_OFFSET, migrationState.getCompletedMigrations());
        encodeLong(initialFrame.content, TOTAL_ELAPSED_TIME_FIELD_OFFSET, migrationState.getTotalElapsedTime());
        clientMessage.add(initialFrame);

        clientMessage.add(END_FRAME.copy());
    }

    public static com.hazelcast.internal.partition.MigrationStateImpl decode(ClientMessage.ForwardFrameIterator iterator) {
        // begin frame
        iterator.next();

        ClientMessage.Frame initialFrame = iterator.next();
        long startTime = decodeLong(initialFrame.content, START_TIME_FIELD_OFFSET);
        int plannedMigrations = decodeInt(initialFrame.content, PLANNED_MIGRATIONS_FIELD_OFFSET);
        int completedMigrations = decodeInt(initialFrame.content, COMPLETED_MIGRATIONS_FIELD_OFFSET);
        long totalElapsedTime = decodeLong(initialFrame.content, TOTAL_ELAPSED_TIME_FIELD_OFFSET);

        fastForwardToEndFrame(iterator);

        return new com.hazelcast.internal.partition.MigrationStateImpl(startTime, plannedMigrations, completedMigrations, totalElapsedTime);
    }
}
