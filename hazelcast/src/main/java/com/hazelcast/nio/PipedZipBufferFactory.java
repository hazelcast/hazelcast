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

import java.io.*;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static com.hazelcast.nio.IOUtil.closeResource;

public final class PipedZipBufferFactory {

    public static DeflatingPipedBuffer createDeflatingBuffer(int compressedDataSize) {
        return createDeflatingBuffer(compressedDataSize, Deflater.DEFAULT_COMPRESSION);
    }

    public static DeflatingPipedBuffer createDeflatingBuffer(int compressedDataSize, int compressionLevel) {
        return new DeflatingPipedBufferImpl(compressedDataSize, compressionLevel);
    }

    public static InflatingPipedBuffer createInflatingBuffer(int compressedDataSize) {
        return new InflatingPipedBufferImpl(compressedDataSize);
    }

    public interface DeflatingPipedBuffer extends PipedZipBufferCommons {
        DataOutput getDataOutput();

        int deflate();
    }

    public interface InflatingPipedBuffer extends PipedZipBufferCommons {
        DataInput getDataInput();

        int inflate() throws DataFormatException;

        int inflate(int length) throws DataFormatException;
    }

    private interface PipedZipBufferCommons {
        ByteBuffer getInputBuffer();

        ByteBuffer getOutputBuffer();

        OutputStream getOutputStream();

        InputStream getInputStream();

        void reset();

        void destroy();
    }

    private static class DeflatingPipedBufferImpl extends PipedZipBufferSupport implements DeflatingPipedBuffer {
        private final Deflater deflater;
        private DataOutputStream dataOutput;

        private DeflatingPipedBufferImpl(int compressedDataSize, int compressionLevel) {
            super(compressedDataSize);
            deflater = new Deflater(compressionLevel);
            dataOutput = new DataOutputStream(getOutputStream());
        }

        public int deflate() {
            try {
                deflater.setInput(uncompressedBuffer.array(), 0, uncompressedBuffer.position());
                deflater.finish();
                final int count = deflater.deflate(compressedBuffer.array());
                return count;
            } finally {
                deflater.reset();
            }
        }

        @Override
        public void reset() {
            super.reset();
            deflater.reset();
        }

        public ByteBuffer getInputBuffer() {
            return uncompressedBuffer;
        }

        public ByteBuffer getOutputBuffer() {
            return compressedBuffer;
        }

        public DataOutput getDataOutput() {
            return dataOutput;
        }

        public void destroy() {
            closeResource(dataOutput);
            dataOutput = null;
            deflater.end();
            super.destroy();
        }
    }

    private static class InflatingPipedBufferImpl extends PipedZipBufferSupport implements InflatingPipedBuffer {
        private final Inflater inflater = new Inflater();
        private DataInputStream dataInput;

        private InflatingPipedBufferImpl(int compressedDataSize) {
            super(compressedDataSize);
            dataInput = new DataInputStream(getInputStream());
        }

        public int inflate() throws DataFormatException {
            return inflate(compressedBuffer.capacity());
        }

        public int inflate(int length) throws DataFormatException {
            try {
                inflater.setInput(compressedBuffer.array(), 0, length);
                final int count = inflater.inflate(uncompressedBuffer.array());
                uncompressedBuffer.limit(count);
                uncompressedBuffer.position(0);
                return count;
            } finally {
                inflater.reset();
            }
        }

        @Override
        public void reset() {
            inflater.reset();
            super.reset();
        }

        public ByteBuffer getInputBuffer() {
            return compressedBuffer;
        }

        public ByteBuffer getOutputBuffer() {
            return uncompressedBuffer;
        }

        public DataInput getDataInput() {
            return dataInput;
        }

        public void destroy() {
            closeResource(dataInput);
            dataInput = null;
            inflater.end();
            super.destroy();
        }
    }

    private static abstract class PipedZipBufferSupport implements PipedZipBufferCommons {
        protected ByteBuffer compressedBuffer;
        protected ByteBuffer uncompressedBuffer;
        protected InputStream inputStream;
        protected OutputStream outputStream;

        private PipedZipBufferSupport(int compressedDataSize) {
            super();
            compressedBuffer = ByteBuffer.allocate(compressedDataSize);
            uncompressedBuffer = ByteBuffer.allocate(compressedDataSize * 10);
            outputStream = IOUtil.newOutputStream(getInputBuffer());
            inputStream = IOUtil.newInputStream(getOutputBuffer());
        }

        public final OutputStream getOutputStream() {
            return outputStream;
        }

        public final InputStream getInputStream() {
            return inputStream;
        }

        public void reset() {
            uncompressedBuffer.clear();
            compressedBuffer.clear();
        }

        public void destroy() {
            closeResource(inputStream);
            closeResource(outputStream);
            compressedBuffer = null;
            uncompressedBuffer = null;
            inputStream = null;
            outputStream = null;
        }
    }
}
