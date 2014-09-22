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
import com.hazelcast.util.QuickMath;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * Class to encode/decode UTF-Strings to and from byte-arrays.
 */
public final class UTFEncoderDecoder {

    private static final int STRING_CHUNK_SIZE = 16 * 1024;

    private static final UTFEncoderDecoder INSTANCE;
    private static final StringValueArrayProviderFactory STRING_VALUE_ARRAY_PROVIDER_FACTORY;

    // TODO Currently ASCII state support is disabled as default because of some failing unit tests.
    // Because this flag is not set for Non-Buffered Data Output classes
    // but results may be compared in unit tests.
    // Buffered Data Output may set this flag
    // but Non-Buffered Data Output class always set this flag to "false".
    // So their results may be different.
    private static final boolean ASCII_AWARE =
            Boolean.parseBoolean(
                    System.getProperty("hazelcast.nio.asciiaware", "false"));

    private static final CharArrayBasedUtfWriter CHAR_ARRAY_BASED_UTF_WRITER =
            new CharArrayBasedUtfWriter();
    private static final StringBasedUtfWriter STRING_BASED_UTF_WRITER =
            new StringBasedUtfWriter();

    static {
        INSTANCE = buildUTFUtil();

        // Try Unsafe based implementation
        StringValueArrayProviderFactory stringValueArrayProviderFactory =
                new UnsafeBasedStringCharProviderFactory();
        // If Unsafe based implementation is not available for usage
        if (!stringValueArrayProviderFactory.isAvailable()) {
            // Try Reflection based implementation
            stringValueArrayProviderFactory = new ReflectionBasedStringCharProviderFactory();
            // If Reflection based implementation is not available for usage
            if (!stringValueArrayProviderFactory.isAvailable()) {
                stringValueArrayProviderFactory = null;
            }
        }
        STRING_VALUE_ARRAY_PROVIDER_FACTORY = stringValueArrayProviderFactory;
    }

    private final StringCreator stringCreator;
    private final boolean hazelcastEnterpriseActive;

    private UTFEncoderDecoder(boolean fastStringCreator) {
        this(fastStringCreator ? buildFastStringCreator() : new DefaultStringCreator(), false);
    }

    private UTFEncoderDecoder(StringCreator stringCreator,
                                       boolean hazelcastEnterpriseActive) {
        this.stringCreator = stringCreator;
        this.hazelcastEnterpriseActive = hazelcastEnterpriseActive;
    }

    public StringCreator getStringCreator() {
        return stringCreator;
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

        final StringValueArrayProvider stringValueArrayProvider =
                STRING_VALUE_ARRAY_PROVIDER_FACTORY != null
                        ? STRING_VALUE_ARRAY_PROVIDER_FACTORY.create(str)
                        : null;
        final UtfWriter utfWriter =
                stringValueArrayProvider != null
                        ? CHAR_ARRAY_BASED_UTF_WRITER
                        : STRING_BASED_UTF_WRITER;

        int length = str.length();
        out.writeInt(length);
        out.writeInt(length);
        if (length > 0) {
            int chunkSize = (length / STRING_CHUNK_SIZE) + 1;
            for (int i = 0; i < chunkSize; i++) {
                int beginIndex = Math.max(0, i * STRING_CHUNK_SIZE - 1);
                int endIndex = Math.min((i + 1) * STRING_CHUNK_SIZE - 1, length);
                utfWriter.writeShortUTF(stringValueArrayProvider, out, str, beginIndex, endIndex, buffer);
            }
        }
    }

    // ********************************************************************* //

    private interface StringValueArrayProviderFactory {

        boolean isAvailable();
        StringValueArrayProvider create(String str);

    }

    private static class UnsafeBasedStringCharProviderFactory
            implements StringValueArrayProviderFactory {

        private static long stringValueFieldOffset = -1;
        private static sun.misc.Unsafe unsafe;

        static {
            if (UnsafeHelper.UNSAFE_AVAILABLE) {
                unsafe = UnsafeHelper.UNSAFE;
                try {
                    stringValueFieldOffset =
                            unsafe.objectFieldOffset(String.class.getDeclaredField("value"));
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            }
        }

        @Override
        public StringValueArrayProvider create(String str) {
            return new UnsafeBasedStringCharProvider(unsafe, stringValueFieldOffset, str);
        }

        @Override
        public boolean isAvailable() {
            return unsafe != null && stringValueFieldOffset != -1;
        }

    }

    private static class ReflectionBasedStringCharProviderFactory
            implements StringValueArrayProviderFactory {

        private static Field valueArrayField;

        static {
            try {
                valueArrayField = String.class.getDeclaredField("value");
                valueArrayField.setAccessible(true);
            } catch (Throwable t) {
                t.printStackTrace();
                valueArrayField = null;
            }
        }

        @Override
        public StringValueArrayProvider create(String str) {
            return new ReflectionBasedStringCharProvider(valueArrayField, str);
        }

        @Override
        public boolean isAvailable() {
            return valueArrayField != null;
        }

    }

    // ********************************************************************* //

    private interface StringValueArrayProvider {

        char[] value();

    }

    private static class UnsafeBasedStringCharProvider
            implements StringValueArrayProvider {

        private char[] value;

        UnsafeBasedStringCharProvider(sun.misc.Unsafe unsafe,
                                      long stringValueFieldOffset, String str) {
            this.value = (char[]) unsafe.getObject(str, stringValueFieldOffset);
        }

        @Override
        public char[] value() {
            return value;
        }

    }

    private static class ReflectionBasedStringCharProvider
            implements StringValueArrayProvider {

        private char[] value;

        ReflectionBasedStringCharProvider(Field valueArrayField, String str) {
            try {
                this.value = (char[]) valueArrayField.get(str);
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        }

        @Override
        public char[] value() {
            return value;
        }

    }

    // ********************************************************************* //

    private interface UtfWriter {

        void writeShortUTF(final StringValueArrayProvider stringCharProvider,
                           final DataOutput out,
                           final String str,
                           final int beginIndex,
                           final int endIndex,
                           final byte[] buffer) throws IOException;

    }

    private static class CharArrayBasedUtfWriter implements UtfWriter {

        //CHECKSTYLE:OFF
        @Override
        public void writeShortUTF(final StringValueArrayProvider stringCharProvider,
                                  final DataOutput out,
                                  final String str,
                                  final int beginIndex,
                                  final int endIndex,
                                  final byte[] buffer) throws IOException {
            final boolean isBufferObjectDataOutput = out instanceof BufferObjectDataOutput;
            final BufferObjectDataOutput bufferObjectDataOutput =
                    isBufferObjectDataOutput ? (BufferObjectDataOutput) out : null;
            final char[] value = stringCharProvider.value();

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
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }

                for (; i < endIndex; i++) {
                    c = value[i]; //value != null ? value[i] : str.charAt(i);
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else if (c > 0x007F) {
                        // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else {
                        // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    }
                }

                out.write(buffer, 0, bufferPos);

                if (isBufferObjectDataOutput) {
                    utfLength = bufferPos;
                }
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = value[i];
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                }

                if (isBufferObjectDataOutput) {
                    utfLength = i - beginIndex;
                }

                for (; i < endIndex; i++) {
                    c = value[i];
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        if (isBufferObjectDataOutput) {
                            utfLength += 2;
                        }
                    } else if (c > 0x007F) {
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
                        // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                        if (isBufferObjectDataOutput) {
                            utfLength++;
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

    private static class StringBasedUtfWriter implements UtfWriter {

        //CHECKSTYLE:OFF
        @Override
        public void writeShortUTF(final StringValueArrayProvider stringValueArrayProvider,
                                  final DataOutput out,
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
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    buffer[bufferPos++] = (byte) c;
                }

                for (; i < endIndex; i++) {
                    c = str.charAt(i);
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        buffer[bufferPos++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else if (c > 0x007F) {
                        // 0x007F < X <= 0x7FFF
                        buffer[bufferPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        buffer[bufferPos++] = (byte) (0x80 | ((c) & 0x3F));
                    } else {
                        // 0x0001 <= X <= 0x007F
                        buffer[bufferPos++] = (byte) c;
                    }
                }

                out.write(buffer, 0, bufferPos);

                if (isBufferObjectDataOutput) {
                    utfLength = bufferPos;
                }
            } else {
                for (i = beginIndex; i < endIndex; i++) {
                    c = str.charAt(i);
                    if (!((c <= 0x007F) && (c >= 0x0001))) {
                        break;
                    }
                    bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                }

                if (isBufferObjectDataOutput) {
                    utfLength = i - beginIndex;
                }

                for (; i < endIndex; i++) {
                    c = str.charAt(i);
                    if (c <= 0) {
                        // X == 0 or 0x007F < X < 0x7FFF
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0xC0 | ((c >> 6) & 0x1F)), out);
                        bufferPos = buffering(buffer, bufferPos,
                                (byte) (0x80 | ((c) & 0x3F)), out);
                        if (isBufferObjectDataOutput) {
                            utfLength += 2;
                        }
                    } else if (c > 0x007F) {
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
                        // 0x0001 <= X <= 0x007F
                        bufferPos = buffering(buffer, bufferPos, (byte) c, out);
                        if (isBufferObjectDataOutput) {
                            utfLength++;
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
        final int utfLength = in.readShort();
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
            while (bufferPos != bufferLimit) {
                c1 = buffer[bufferPos++] & 0xFF;
                if (c1 > 127) {
                    bufferPos--;
                    break;
                }
                data[charArrCount++] = (char) c1;
            }

            readCount = bufferPos - 1;

            // Means that, 1. loop is finished since "bufferPos" is equal to "minUtfLenght"
            // and buffer capacity may be not enough to serve the requested byte.
            // So, we should get requested byte via "buffered" method by checking buffer and
            // reloading it from DataInput if it is empty.
            if (bufferPos == bufferLimit) {
                bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                c1 = buffer[0] & 0xFF;
            }

            while (readCount < utfLength) {
                cTemp = c1 >> 4;
                if (cTemp >> 3 == 0) {
                    // ((cTemp & 0xF8) == 0) or (cTemp <= 7 && cTemp >= 0)
                        /* 0xxxxxxx */
                    data[charArrCount++] = (char) c1;
                    readCount++;
                } else if (cTemp == 12 || cTemp == 13) {
                        /* 110x xxxx 10xx xxxx */
                    if (readCount + 1 > utfLength) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                    c2 = buffer[0] & 0xFF;
                    if ((c2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + beginIndex + readCount + 1);
                    }
                    data[charArrCount++] = (char) (((c1 & 0x1F) << 6) | (c2 & 0x3F));
                    readCount += 2;
                } else if (cTemp == 14) {
                        /* 1110 xxxx 10xx xxxx 10xx xxxx */
                    if (readCount + 2 > utfLength) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                    c2 = buffer[0] & 0xFF;
                    bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                    c3 = buffer[0] & 0xFF;
                    if (((c2 & 0xC0) != 0x80) || ((c3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (beginIndex + readCount + 1));
                    }
                    data[charArrCount++] = (char) (((c1 & 0x0F) << 12)
                            | ((c2 & 0x3F) << 6) | ((c3 & 0x3F)));
                    readCount += 3;
                } else {
                        /* 10xx xxxx, 1111 xxxx */
                    throw new UTFDataFormatException(
                            "malformed input around byte " + (beginIndex + readCount));
                }

                bufferPos = buffered(buffer, bufferPos, utfLength, readCount, in);
                c1 = buffer[0] & 0xFF;
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
            if (c <= 0) {
                // X == 0 or 0x007F < X < 0x7FFF
                utfLength += 2;
            } else if (c > 0x007F) {
                // 0x007F < X <= 0x7FFF
                utfLength += 3;
            } else {
                // 0x0001 <= X <= 0x007F
                utfLength++;
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
            if (c <= 0) {
                // X == 0 or 0x007F < X < 0x7FFF
                utfLength += 2;
            } else if (c > 0x007F) {
                // 0x007F < X <= 0x7FFF
                utfLength += 3;
            } else {
                // 0x0001 <= X <= 0x007F
                utfLength++;
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

    public static boolean useOldStringConstructor() {
        try {
            Class<String> clazz = String.class;
            clazz.getDeclaredConstructor(int.class, int.class, char[].class);
            return true;
        } catch (Throwable t) {
            Logger.
                    getLogger(UTFEncoderDecoder.class).
                    finest("Old String constructor doesn't seem available", t);
        }
        return false;
    }

    private static UTFEncoderDecoder buildUTFUtil() {
        try {
            Class<?> clazz =
                    Class.forName("com.hazelcast.nio.utf8.EnterpriseStringCreator");
            Method method = clazz.getDeclaredMethod("findBestStringCreator");
            return new UTFEncoderDecoder(
                    (StringCreator) method.invoke(clazz), true);
        } catch (Throwable t) {
            Logger.
                    getLogger(UTFEncoderDecoder.class).
                    finest("EnterpriseStringCreator not available on classpath", t);
        }
        boolean faststringEnabled =
                Boolean.parseBoolean(
                        System.getProperty("hazelcast.nio.faststring", "true"));
        return new UTFEncoderDecoder(
                faststringEnabled
                        ? buildFastStringCreator()
                        : new DefaultStringCreator(), false);
    }

    private static StringCreator buildFastStringCreator() {
        try {
            // Give access to the package private String constructor
            Constructor<String> constructor = null;
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

    private static class DefaultStringCreator implements UTFEncoderDecoder.StringCreator {

        @Override
        public String buildString(final char[] chars) {
            return new String(chars);
        }

    }

    private static class FastStringCreator implements UTFEncoderDecoder.StringCreator {

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

    public interface StringCreator {

        String buildString(final char[] chars);

    }

}
