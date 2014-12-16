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

package com.hazelcast.nio;

import com.hazelcast.logging.Logger;
import com.hazelcast.util.EmptyStatement;
import com.hazelcast.util.QuickMath;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

/**
 * Class to encode/decode UTF-Strings to and from byte-arrays.
 */
public final class UTFEncoderDecoder {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    private static final UTFEncoderDecoder INSTANCE;

    // Because this flag is not set for Non-Buffered Data Output classes
    // but results may be compared in unit tests.
    // Buffered Data Output may set this flag
    // but Non-Buffered Data Output class always set this flag to "false".
    // So their results may be different.
    private static final boolean ASCII_AWARE =
            Boolean.parseBoolean(System.getProperty("hazelcast.nio.asciiaware", "false"));

    static {
        INSTANCE = buildUTFUtil();
    }

    private final StringCreator stringCreator;
    private final UtfWriter utfWriter;
    private final boolean hazelcastEnterpriseActive;

    UTFEncoderDecoder(StringCreator stringCreator, UtfWriter utfWriter) {
        this(stringCreator, utfWriter, false);
    }

    UTFEncoderDecoder(StringCreator stringCreator, UtfWriter utfWriter, boolean hazelcastEnterpriseActive) {
        this.stringCreator = stringCreator;
        this.utfWriter = utfWriter;
        this.hazelcastEnterpriseActive = hazelcastEnterpriseActive;
    }

    public boolean isHazelcastEnterpriseActive() {
        return hazelcastEnterpriseActive;
    }

    public static void writeUTF(final DataOutput out,
                                final String str,
                                final byte[] buffer) throws IOException {
        INSTANCE.writeUTF0(out, str, buffer);
    }

    public static String readUTF(final DataInput in,
                                 final byte[] buffer) throws IOException {
        return INSTANCE.readUTF0(in, buffer);
    }

    // ********************************************************************* //

    public void writeUTF0(final DataOutput out,
                          final String str,
                          final byte[] buffer) throws IOException {
        if (!QuickMath.isPowerOfTwo(buffer.length)) {
            throw new IllegalArgumentException(
                    "Size of the buffer has to be power of two, was " + buffer.length);
        }
        boolean isNull = str == null;
        out.writeBoolean(isNull);
        if (isNull) {
            return;
        }

        int length = str.length();
        out.writeInt(length);
        out.writeInt(length);
        if (length > 0) {
            int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                utfWriter.writeShortUTF(out, str, beginIndex, endIndex, buffer);
            }
        }
    }

    // ********************************************************************* //

    interface UtfWriter {

        void writeShortUTF(final DataOutput out,
                           final String str,
                           final int beginIndex,
                           final int endIndex,
                           final byte[] buffer) throws IOException;

    }

    private abstract static class AbstractCharArrayUtfWriter implements UtfWriter {

        //CHECKSTYLE:OFF
        @Override
        public final void writeShortUTF(final DataOutput out,
                                        final String str,
                                        final int beginIndex,
                                        final int endIndex,
                                        final byte[] buffer) throws IOException {
            final boolean isBufferObjectDataOutput = out instanceof BufferObjectDataOutput;
            final BufferObjectDataOutput bufferObjectDataOutput =
                    isBufferObjectDataOutput ? (BufferObjectDataOutput) out : null;
            final char[] value = getCharArray(str);

            int i;
            int c;
            int bufferPos = 0;
            int utfLength = 0;
            int utfLengthLimit;
            int pos = 0;

            if (isBufferObjectDataOutput) {
                // At most, one character can hold 3 bytes
                utfLengthLimit = str.length() * 3;

                // We save current position of buffer data output.
                // Then we write the length of UTF and ASCII state to here
                pos = bufferObjectDataOutput.position();

                // Moving position explicitly is not good way
                // since it may cause overflow exceptions for example "ByteArrayObjectDataOutput".
                // So, write dummy data and let DataOutput handle it by expanding or etc ...
                bufferObjectDataOutput.writeShort(0);
                if (ASCII_AWARE) {
                    bufferObjectDataOutput.writeBoolean(false);
                }
            } else {
                utfLength = calculateUtf8Length(value, beginIndex, endIndex);
                if (utfLength > 65535) {
                    throw new UTFDataFormatException(
                            "encoded string too long:" + utfLength + " bytes");
                }

                utfLengthLimit = utfLength;

                out.writeShort(utfLength);

                if (ASCII_AWARE) {
                    // We cannot determine that all characters are ASCII or not without iterating over it
                    // So, we mark it as not ASCII, so all characters will be checked.
                    out.writeBoolean(false);
                }
            }

            if (buffer.length >= utfLengthLimit) {
                for (i = beginIndex; i < endIndex; i++) {
                    c = value[i];
                    if (!(c <= 0x007F && c >= 0x0001)) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }

                for (; i < endIndex; i++) {
                    c = value[i];
                    if (c <= 0x007F && c >= 0x0001) {
                        buffer[bufferPos++] = (byte) c;
                    } else if (c > 0x07FF) {
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else {
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    }
                }

                out.write(buffer, 0, bufferPos);

                if (isBufferObjectDataOutput) {
                    utfLength = bufferPos;
                }
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = value[i];
                    if (!(c <= 0x007F && c >= 0x0001)) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                }

                if (isBufferObjectDataOutput) {
                    utfLength = i - beginIndex;
                }

                for (; i < endIndex; i++) {
                    c = value[i];
                    if (c <= 0x007F && c >= 0x0001) {
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                        if (isBufferObjectDataOutput) {
                            utfLength++;
                        }
                    } else if (c > 0x07FF) {
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        if (isBufferObjectDataOutput) {
                            utfLength += 3;
                        }
                    } else {
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        if (isBufferObjectDataOutput) {
                            utfLength += 2;
                        }
                    }
                }
                int length = bufferPos % buffer.length;
                out.write(buffer, 0, length == 0 ? buffer.length : length);
            }

            if (isBufferObjectDataOutput) {
                if (utfLength > 65535) {
                    throw new UTFDataFormatException(
                            "encoded string too long:" + utfLength + " bytes");
                }

                // Write the length of UTF to saved position before
                bufferObjectDataOutput.writeShort(pos, utfLength);

                // Write the ASCII status of UTF to saved position before
                if (ASCII_AWARE) {
                    bufferObjectDataOutput.writeBoolean(pos + 2, utfLength == str.length());
                }
            }
        }

        protected abstract boolean isAvailable();

        protected abstract char[] getCharArray(String str);
        //CHECKSTYLE:ON
    }

    static class UnsafeBasedCharArrayUtfWriter extends AbstractCharArrayUtfWriter {

        private static final sun.misc.Unsafe UNSAFE = UnsafeHelper.UNSAFE;
        private static final long VALUE_FIELD_OFFSET;

        static {
            long offset = -1;
            if (UnsafeHelper.UNSAFE_AVAILABLE) {
                try {
                    offset = UNSAFE.objectFieldOffset(String.class.getDeclaredField("value"));
                } catch (Throwable t) {
                    EmptyStatement.ignore(t);
                }
            }
            VALUE_FIELD_OFFSET = offset;
        }

        @Override
        public boolean isAvailable() {
            return UnsafeHelper.UNSAFE_AVAILABLE && VALUE_FIELD_OFFSET != -1;
        }

        @Override
        protected char[] getCharArray(String str) {
            char[] chars = (char[]) UNSAFE.getObject(str, VALUE_FIELD_OFFSET);
            if (chars.length > str.length()) {
                // substring detected!
                // jdk6 substring shares the same value array
                // with the original string (this is not the case for jdk7+)
                // we need to get copy of substring array
                chars = str.toCharArray();
            }
            return chars;
        }
    }

    static class ReflectionBasedCharArrayUtfWriter extends AbstractCharArrayUtfWriter {

        private static final Field VALUE_FIELD;

        static {
            Field field;
            try {
                field = String.class.getDeclaredField("value");
                field.setAccessible(true);
            } catch (Throwable t) {
                EmptyStatement.ignore(t);
                field = null;
            }
            VALUE_FIELD = field;
        }

        @Override
        public boolean isAvailable() {
            return VALUE_FIELD != null;
        }

        @Override
        protected char[] getCharArray(String str) {
            try {
                char[] chars = (char[]) VALUE_FIELD.get(str);
                if (chars.length > str.length()) {
                    // substring detected!
                    // jdk6 substring shares the same value array
                    // with the original string (this is not the case for jdk7+)
                    // we need to get copy of substring array
                    chars = str.toCharArray();
                }
                return chars;
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    static class StringBasedUtfWriter implements UtfWriter {

        //CHECKSTYLE:OFF
        @Override
        public void writeShortUTF(final DataOutput out,
                                  final String str,
                                  final int beginIndex,
                                  final int endIndex,
                                  final byte[] buffer) throws IOException {
            final boolean isBufferObjectDataOutput = out instanceof BufferObjectDataOutput;
            final BufferObjectDataOutput bufferObjectDataOutput =
                    isBufferObjectDataOutput ? (BufferObjectDataOutput) out : null;

            int i;
            int c;
            int bufferPos = 0;
            int utfLength = 0;
            int utfLengthLimit;
            int pos = 0;

            if (isBufferObjectDataOutput) {
                // At most, one character can hold 3 bytes
                utfLengthLimit = str.length() * 3;

                // We save current position of buffer data output.
                // Then we write the length of UTF and ASCII state to here
                pos = bufferObjectDataOutput.position();

                // Moving position explicitly is not good way
                // since it may cause overflow exceptions for example "ByteArrayObjectDataOutput".
                // So, write dummy data and let DataOutput handle it by expanding or etc ...
                bufferObjectDataOutput.writeShort(0);
                if (ASCII_AWARE) {
                    bufferObjectDataOutput.writeBoolean(false);
                }
            } else {
                utfLength = calculateUtf8Length(str, beginIndex, endIndex);
                if (utfLength > 65535) {
                    throw new UTFDataFormatException(
                            "encoded string too long:" + utfLength + " bytes");
                }
                utfLengthLimit = utfLength;

                out.writeShort(utfLength);
                if (ASCII_AWARE) {
                    // We cannot determine that all characters are ASCII or not without iterating over it
                    // So, we mark it as not ASCII, so all characters will be checked.
                    out.writeBoolean(false);
                }
            }

            if (buffer.length >= utfLengthLimit) {
                for (i = beginIndex; i < endIndex; i++) {
                    c = str.charAt(i);
                    if (!(c <= 0x007F && c >= 0x0001)) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }

                for (; i < endIndex; i++) {
                    c = str.charAt(i);
                    if ((c >= 0x0001) && (c <= 0x007F)) {
                        // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    } else if (c > 0x07FF) {
                        // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else {
                        // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    }
                }

                out.write(buffer, 0, bufferPos);

                if (isBufferObjectDataOutput) {
                    utfLength = bufferPos;
                }
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = str.charAt(i);
                    if (!(c <= 0x007F && c >= 0x0001)) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                }

                if (isBufferObjectDataOutput) {
                    utfLength = i - beginIndex;
                }

                for (; i < endIndex; i++) {
                    c = str.charAt(i);
                    if (c <= 0x007F && c >= 0x0001) {
                        // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                        if (isBufferObjectDataOutput) {
                            utfLength++;
                        }
                    } else if (c > 0x07FF) {
                        // 0x007F < X <= 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xE0 | ((c >> 12) & 0x0F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c >> 6) & 0x3F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        if (isBufferObjectDataOutput) {
                            utfLength += 3;
                        }
                    } else {
                        // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        if (isBufferObjectDataOutput) {
                            utfLength += 2;
                        }
                    }
                }
                int length = bufferPos % buffer.length;
                out.write(buffer, 0, length == 0 ? buffer.length : length);
            }

            if (isBufferObjectDataOutput) {
                if (utfLength > 65535) {
                    throw new UTFDataFormatException(
                            "encoded string too long:" + utfLength + " bytes");
                }

                // Write the length of UTF to saved position before
                bufferObjectDataOutput.writeShort(pos, utfLength);

                // Write the ASCII status of UTF to saved position before
                if (ASCII_AWARE) {
                    bufferObjectDataOutput.writeBoolean(pos + 2, utfLength == str.length());
                }
            }
        }
        //CHECKSTYLE:ON

    }

    // ********************************************************************* //

    public String readUTF0(final DataInput in, final byte[] buffer) throws IOException {
        if (!QuickMath.isPowerOfTwo(buffer.length)) {
            throw new IllegalArgumentException(
                    "Size of the buffer has to be power of two, was " + buffer.length);
        }
        boolean isNull = in.readBoolean();
        if (isNull) {
            return null;
        }
        int length = in.readInt();
        int lengthCheck = in.readInt();
        if (length != lengthCheck) {
            throw new UTFDataFormatException(
                    "Length check failed, maybe broken bytestream or wrong stream position");
        }
        final char[] data = new char[length];
        if (length > 0) {
            int chunkSize = length / STRING_CHUNK_SIZE + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                readShortUTF(in, data, beginIndex, buffer);
            }
        }
        return stringCreator.buildString(data);
    }

    //CHECKSTYLE:OFF
    private void readShortUTF(final DataInput in,
                              final char[] data,
                              final int beginIndex,
                              final byte[] buffer) throws IOException {
        final int utfLength = in.readShort() & 0xFFFF;
        final boolean allAscii = ASCII_AWARE ? in.readBoolean() : false;
        // buffer[0] is used to hold read data
        // so actual useful length of buffer is as "length - 1"
        final int minUtfLenght = Math.min(utfLength, buffer.length - 1);
        final int bufferLimit = minUtfLenght + 1;
        int readCount = 0;
        // We use buffer[0] to hold read data, so position starts from 1
        int bufferPos = 1;
        int c1 = 0;
        int c2 = 0;
        int c3 = 0;
        int cTemp = 0;
        int charArrCount = beginIndex;

        // The first readable data is at 1. index since 0. index is used to hold read data.
        in.readFully(buffer, 1, minUtfLenght);

        if (allAscii) {
            while (bufferPos != bufferLimit) {
                data[charArrCount++] = (char)(buffer[bufferPos++] & 0xFF);
            }

            for (readCount = bufferPos - 1; readCount < utfLength; readCount++) {
                bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                data[charArrCount++] = (char) (buffer[0] & 0xFF);
            }
        } else {
            c1 = buffer[bufferPos++] & 0xFF;
            while (bufferPos != bufferLimit) {
                if (c1 > 127) {
                    break;
                }
                data[charArrCount++] = (char) c1;
                c1 = buffer[bufferPos++] & 0xFF;
            }

            bufferPos--;
            readCount = bufferPos - 1;

            while (readCount < utfLength) {
                bufferPos = buffered(buffer, bufferPos, utfLength, readCount++, in);
                c1 = buffer[0] & 0xFF;
                cTemp = c1 >> 4;
                if (cTemp >> 3 == 0) {
                    // ((cTemp & 0xF8) == 0) or (cTemp <= 7 && cTemp >= 0)
                    /* 0xxxxxxx */
                    data[charArrCount++] = (char) c1;
                } else if (cTemp == 12 || cTemp == 13) {
                    /* 110x xxxx 10xx xxxx */
                    if (readCount + 1 > utfLength) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount++, in);
                    c2 = buffer[0] & 0xFF;
                    if ((c2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + beginIndex + readCount + 1);
                    }
                    data[charArrCount++] = (char) (((c1 & 0x1F) << 6) | (c2 & 0x3F));
                } else if (cTemp == 14) {
                    /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    if (readCount + 2 > utfLength) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount++, in);
                    c2 = buffer[0] & 0xFF;
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount++, in);
                    c3 = buffer[0] & 0xFF;
                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (beginIndex + readCount + 1));
                    }
                    data[charArrCount++] = (char) (((c1 & 0x0F) << 12)
                            | ((c2 & 0x3F) << 6) | ((c3 & 0x3F)));
                } else {
                    /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException(
                            "malformed input around byte " + (beginIndex + readCount));
                }
            }
        }
    }
    //CHECKSTYLE:ON

    // ********************************************************************* //

    private static int calculateUtf8Length(final char[] value,
                                           final int beginIndex,
                                           final int endIndex) {
        int utfLength = 0;
        for (int i = beginIndex; i < endIndex; i++) {
            int c = value[i];
            if (c <= 0x007F && c >= 0x0001) {
                utfLength += 1;
            } else if (c > 0x07FF) {
                utfLength += 3;
            } else {
                utfLength += 2;
            }
        }
        return utfLength;
    }

    private static int calculateUtf8Length(final String str,
                                           final int beginIndex,
                                           final int endIndex) {
        int utfLength = 0;
        for (int i = beginIndex; i < endIndex; i++) {
            int c = str.charAt(i);
            if (c <= 0x007F && c >= 0x0001) {
                utfLength += 1;
            } else if (c > 0x07FF) {
                utfLength += 3;
            } else {
                utfLength += 2;
            }
        }
        return utfLength;
    }

    private static int buffering(final byte[] buffer,
                                 final int pos,
                                 final byte value,
                                 final DataOutput out) throws IOException {
        try {
            buffer[pos] = value;
            return pos + 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Array bounds check by programmatically is not needed like
            // "if (pos < buffer.length)".
            // JVM checks instead of us, so it is unnecessary.
            out.write(buffer, 0, buffer.length);
            buffer[0] = value;
            return 1;
        }
    }

    private int buffered(final byte[] buffer,
                         final int pos,
                         final int utfLength,
                         final int readCount,
                         final DataInput in) throws IOException {
        try {
            // 0. index of buffer is used to hold read data
            // so copy read data to there.
            buffer[0] = buffer[pos];
            return pos + 1;
        } catch (ArrayIndexOutOfBoundsException e) {
            // Array bounds check by programmatically is not needed like
            // "if (pos < buffer.length)".
            // JVM checks instead of us, so it is unnecessary.
            in.readFully(buffer, 1,
                    Math.min(buffer.length - 1, utfLength - readCount));
            // The first readable data is at 1. index since 0. index is used to
            // hold read data.
            // So the next one will be 2. index.
            buffer[0] = buffer[1];
            return 2;
        }
    }

    private static boolean useOldStringConstructor() {
        try {
            Class<String> clazz = String.class;
            clazz.getDeclaredConstructor(int.class, int.class, char[].class);
            return true;
        } catch (Throwable t) {
            Logger.getLogger(UTFEncoderDecoder.class).
                    finest("Old String constructor doesn't seem available", t);
        }
        return false;
    }

    private static UTFEncoderDecoder buildUTFUtil() {
        UtfWriter utfWriter = createUtfWriter();
        StringCreator stringCreator = createStringCreator();
        return new UTFEncoderDecoder(stringCreator, utfWriter, false);
    }

    static StringCreator createStringCreator() {
        boolean fastStringEnabled = Boolean.parseBoolean(System.getProperty("hazelcast.nio.faststring", "true"));
        return createStringCreator(fastStringEnabled);
    }

    static StringCreator createStringCreator(boolean fastStringEnabled) {
        return fastStringEnabled ? buildFastStringCreator() : new DefaultStringCreator();
    }

    static UtfWriter createUtfWriter() {
        // Try Unsafe based implementation
        UnsafeBasedCharArrayUtfWriter unsafeBasedUtfWriter = new UnsafeBasedCharArrayUtfWriter();
        if (unsafeBasedUtfWriter.isAvailable()) {
            return unsafeBasedUtfWriter;
        }

        // If Unsafe based implementation is not available for usage
        // Try Reflection based implementation
        ReflectionBasedCharArrayUtfWriter reflectionBasedUtfWriter = new ReflectionBasedCharArrayUtfWriter();
        if (reflectionBasedUtfWriter.isAvailable()) {
            return reflectionBasedUtfWriter;
        }

        // If Reflection based implementation is not available for usage
        return new StringBasedUtfWriter();
    }

    private static StringCreator buildFastStringCreator() {
        try {
            // Give access to the package private String constructor
            Constructor<String> constructor;
            if (UTFEncoderDecoder.useOldStringConstructor()) {
                constructor =
                        String.class.getDeclaredConstructor(int.class, int.class, char[].class);
            } else {
                constructor =
                        String.class.getDeclaredConstructor(char[].class, boolean.class);
            }
            if (constructor != null) {
                constructor.setAccessible(true);
                return new FastStringCreator(constructor);
            }
        } catch (Throwable t) {
            Logger.
                    getLogger(UTFEncoderDecoder.class).
                    finest("No fast string creator seems to available, falling back to reflection", t);
        }
        return null;
    }

    interface StringCreator {
        String buildString(final char[] chars);
    }

    private static class DefaultStringCreator implements StringCreator {
        @Override
        public String buildString(final char[] chars) {
            return new String(chars);
        }
    }

    private static class FastStringCreator implements StringCreator {

        private final Constructor<String> constructor;
        private final boolean useOldStringConstructor;

        public FastStringCreator(Constructor<String> constructor) {
            this.constructor = constructor;
            this.useOldStringConstructor = constructor.getParameterTypes().length == 3;
        }

        @Override
        public String buildString(final char[] chars) {
            try {
                if (useOldStringConstructor) {
                    return constructor.newInstance(0, chars.length, chars);
                } else {
                    return constructor.newInstance(chars, Boolean.TRUE);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}
