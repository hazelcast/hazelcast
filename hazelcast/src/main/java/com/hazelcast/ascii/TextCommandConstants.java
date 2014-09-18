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

package com.hazelcast.ascii;

import static com.hazelcast.util.StringUtil.stringToBytes;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("MS_MUTABLE_ARRAY")
public final class TextCommandConstants {

    public static final byte[] SPACE = stringToBytes(" ");
    public static final byte[] RETURN = stringToBytes("\r\n");
    public static final byte[] FLAG_ZERO = stringToBytes(" 0 ");
    public static final byte[] VALUE_SPACE = stringToBytes("VALUE ");
    public static final byte[] DELETED = stringToBytes("DELETED\r\n");
    public static final byte[] STORED = stringToBytes("STORED\r\n");
    public static final byte[] TOUCHED = stringToBytes("TOUCHED\r\n");
    public static final byte[] NOT_STORED = stringToBytes("NOT_STORED\r\n");
    public static final byte[] NOT_FOUND = stringToBytes("NOT_FOUND\r\n");
    public static final byte[] RETURN_END = stringToBytes("\r\nEND\r\n");
    public static final byte[] END = stringToBytes("END\r\n");
    public static final byte[] ERROR = stringToBytes("ERROR");
    public static final byte[] CLIENT_ERROR = stringToBytes("CLIENT_ERROR ");
    public static final byte[] SERVER_ERROR = stringToBytes("SERVER_ERROR ");

    private static final int SECOND = 60;
    private static final int MINUTE = 60;
    private static final int HOUR = 24;
    private static final int MONTH = 30;

    private static int monthSeconds = SECOND * MINUTE * HOUR * MONTH;

    private TextCommandConstants() {

    }

    public static int getMonthSeconds() {
        return monthSeconds;
    }

    public enum TextCommandType {
        GET((byte) 0),
        PARTIAL_GET((byte) 1),
        GETS((byte) 2),
        SET((byte) 3),
        APPEND((byte) 4),
        PREPEND((byte) 5),
        ADD((byte) 6),
        REPLACE((byte) 7),
        DELETE((byte) 8),
        QUIT((byte) 9),
        STATS((byte) 10),
        GET_END((byte) 11),
        ERROR_CLIENT((byte) 12),
        ERROR_SERVER((byte) 13),
        UNKNOWN((byte) 14),
        VERSION((byte) 15),
        TOUCH((byte) 16),
        INCREMENT((byte) 17),
        DECREMENT((byte) 18),
        HTTP_GET((byte) 30),
        HTTP_POST((byte) 31),
        HTTP_PUT((byte) 32),
        HTTP_DELETE((byte) 33),
        NO_OP((byte) 98),
        STOP((byte) 99);

        final byte value;

        TextCommandType(byte type) {
            value = type;
        }

        public byte getValue() {
            return value;
        }
    }
}
