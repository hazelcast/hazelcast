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

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

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
		ByteBuffer getInputBuffer() ;
	    ByteBuffer getOutputBuffer() ;
	    OutputStream getOutputStream() ;
	    InputStream getInputStream() ;
	    void reset() ;
	}
	
	private static class DeflatingPipedBufferImpl extends PipedZipBufferSupport implements DeflatingPipedBuffer {
	    private final Deflater deflater;
	    private final DataOutput dataOutput;
	    
	    private DeflatingPipedBufferImpl(int compressedDataSize, int compressionLevel) {
	    	super(compressedDataSize);
	    	deflater = new Deflater(compressionLevel);
	    	dataOutput = new DataOutputStream(getOutputStream());
	    }
	    
	    public int deflate() {
	        deflater.setInput(uncompressedBuffer.array(), 0, uncompressedBuffer.position());
	        deflater.finish();
	        final int count = deflater.deflate(compressedBuffer.array());
	        deflater.reset();
	    	return count;
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
	}
	
	private static class InflatingPipedBufferImpl extends PipedZipBufferSupport implements InflatingPipedBuffer {
	    private final Inflater inflater = new Inflater();
	    private final DataInput dataInput;
	    
	    private InflatingPipedBufferImpl(int compressedDataSize) {
	    	super(compressedDataSize);
	    	dataInput = new DataInputStream(getInputStream());
	    }
	    
	    public int inflate() throws DataFormatException {
	    	return inflate(compressedBuffer.capacity());
	    }
	    
	    public int inflate(int length) throws DataFormatException {
	    	inflater.setInput(compressedBuffer.array(), 0, length);
	        final int count = inflater.inflate(uncompressedBuffer.array());
	        uncompressedBuffer.limit(count);
	        uncompressedBuffer.position(0);
	        inflater.reset();
	        return count;
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
	}
	
	private static abstract class PipedZipBufferSupport implements PipedZipBufferCommons {
		protected final ByteBuffer compressedBuffer ;
		protected final ByteBuffer uncompressedBuffer ;
		protected final InputStream inputStream ;
		protected final OutputStream outputStream;
		
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
	}
}
