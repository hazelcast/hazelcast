/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.nio;

import com.hazelcast.impl.ThreadContext;

import java.io.*;
import java.nio.ByteBuffer;

public final class IOUtil {

    public static OutputStream newOutputStream(final ByteBuffer buf) {
        return new OutputStream() {
            public void write(int b) throws IOException {
                buf.put((byte) b);
            }

            public void write(byte[] bytes, int off, int len) throws IOException {
                buf.put(bytes, off, len);
            }
        };
    }

    public static InputStream newInputStream(final ByteBuffer buf) {
        return new InputStream() {
            public int read() throws IOException {
                if (!buf.hasRemaining()) {
                    return -1;
                }
                return buf.get() & 0xff;
            }

            public int read(byte[] bytes, int off, int len) throws IOException {
                len = Math.min(len, buf.remaining());
                buf.get(bytes, off, len);
                return len;
            }
        };
    }

    public static int copyToHeapBuffer(ByteBuffer src, ByteBuffer dest) {
        if (src == null) return 0;
        int n = Math.min(src.remaining(), dest.remaining());
        if (n > 0) {
            if (n < 16) {
                for (int i = 0; i < n; i++) {
                    dest.put(src.get());
                }
            } else {
                int srcPosition = src.position();
                int destPosition = dest.position();
                System.arraycopy(src.array(), srcPosition, dest.array(), destPosition, n);
                src.position(srcPosition + n);
                dest.position(destPosition + n);
            }
        }
        return n;
    }

    public static int copyToDirectBuffer(ByteBuffer src, ByteBuffer dest) {
        int n = Math.min(src.remaining(), dest.remaining());
        if (n > 0) {
            dest.put(src.array(), src.position(), n);
            src.position(src.position() + n);
        }
        return n;
    }

    public static void writeLongString(DataOutputStream dos, String str) throws IOException {
        int chunk = 1000;
        int count = str.length() / chunk;
        int remaining = str.length() - (count * chunk);
        dos.writeInt(count + ((remaining > 0) ? 1 : 0));
        for (int i = 0; i < count; i++) {
            dos.writeUTF(str.substring(i * chunk, (i + 1) * chunk));
        }
        if (remaining > 0) {
            dos.writeUTF(str.substring(count * chunk));
        }
    }

    public static String readLongString(DataInputStream in) throws IOException {
        int count = in.readInt();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(in.readUTF());
        }
        return sb.toString();
    }

    public static void putBoolean(ByteBuffer bb, boolean value) {
        bb.put((byte) (value ? 1 : 0));
    }

    public static boolean getBoolean(ByteBuffer bb) {
        return bb.get() == 1;
    }

    public static Data toData(Object obj) {
        return ThreadContext.get().toData(obj);
    }

    public static long getLong(Data longData) {
        byte[] b = longData.buffer;
        ByteBuffer current = ByteBuffer.wrap(b);
        current.get(); // type
        return current.getLong();
    }

    public static Data addDelta(Data longData, long delta) {
        long longValue = (Long) toObject(longData);
        return toData(longValue + delta);
    }

    public static Object toObject(Data data) {
        return ThreadContext.get().toObject(data);
    }

    public static Object toObject(DataHolder dataHolder) {
        return toObject(dataHolder.toData());
    }
}
