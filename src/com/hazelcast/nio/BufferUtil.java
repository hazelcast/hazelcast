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

package com.hazelcast.nio;

import com.hazelcast.impl.ThreadContext;

import java.nio.ByteBuffer;

public final class BufferUtil {

    public static int copyToHeapBuffer(ByteBuffer src, ByteBuffer dest) {
        int n = Math.min(src.remaining(), dest.remaining());
        if (n > 0) {
            int srcPosition = src.position();
            int destPosition = dest.position();

            int ixSrc = srcPosition + src.arrayOffset();
            int ixDest = destPosition + dest.arrayOffset();

            System.arraycopy(src.array(), ixSrc, dest.array(), ixDest, n);
            src.position(srcPosition + n);
            dest.position(destPosition + n);
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

    public static void putBoolean(ByteBuffer bb, boolean value) {
        bb.put((byte) (value ? 1 : 0));
    }

    public static boolean getBoolean(ByteBuffer bb) {
        return bb.get() == 1;
    }


    public static Data toData(Object obj) {
        return ThreadContext.get().toData(obj);
    }

    public static Object toObject(Data data) {
        return ThreadContext.get().toObject(data);
    }

    public static Data doHardCopy(Data from) {
        if (from == null || from.size == 0)
            return null;
        Data newData = BufferUtil.createNewData();
        BufferUtil.doHardCopy(from, newData);
        return newData;
    }

    public static void doHardCopy(Data from, Data to) {
        to.setNoData();
        copyHard(from, to);
    }

    public static void doSoftCopy(Data from, Data to) {
        to.setNoData();
        moveContent(from, to);
        from.setNoData();
    }

    public static void doSet(Data from, Data to) {
        to.setNoData();
        moveContent(from, to);
        from.setNoData();
    }

    public static Data doTake(Data target) {
        if (target == null || target.size == 0)
            return null;
        Data newData = createNewData();
        moveContent(target, newData);
        target.setNoData();
        return newData;
    }

    public static void moveContent(Data from, Data to) {
        int len = from.lsData.size();
        for (int i = 0; i < len; i++) {
            ByteBuffer bb = from.lsData.get(i);
            to.lsData.add(bb);
            to.size += bb.limit();
        }
        from.lsData.clear();
    }

    public static void copyHard(Data from, Data to) {
        to.setNoData();
        int len = from.lsData.size();
        for (int i = 0; i < len; i++) {
            ByteBuffer bb = from.lsData.get(i);
            ByteBuffer bbTo = obtainEmptyBuffer();
            bbTo.put(bb.array(), 0, bb.limit());
            to.add(bbTo);
            bbTo.flip();
        }
        if (from.size != to.size)
            throw new RuntimeException("size doesn't match: src " + from.size + " dest: " + to.size);
    }

    public static ByteBuffer obtainEmptyBuffer() {
        return ThreadContext.get().getBufferPool().obtain();
    }

    public static Data createNewData() {
        return new Data();
    }

}
