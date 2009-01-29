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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UTFDataFormatException;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BuffersOutputStream extends OutputStream implements DataOutput {

	protected static Logger logger = Logger.getLogger(BuffersOutputStream.class.getName());

	ByteBuffer bb = null;

	BufferProvider bufferProvider = null;

	boolean first = false;

	private static final boolean debug = false;

	private final byte writeBuffer[] = new byte[8];

	public BuffersOutputStream() {
	}

	@Override
	public void flush() {
		if (bb != null) {
			if (first) {
				bb = null;
				first = false;
			} else
				bufferProvider.addBuffer(bb);
		}
	}

	public void reset() {
		bb = null;
		first = false;
	}

	public void setBufferProvider(final BufferProvider bufferProvider) {
		this.bufferProvider = bufferProvider;
	}

	@Override
	public void write(final byte b[], final int off, final int len) {
		if ((off < 0) || (len < 0) || ((off + len) < 0)) {
			throw new IndexOutOfBoundsException();
		} else if (len == 0) {
			return;
		}
		if (bb == null) {
			bb = bufferProvider.takeEmptyBuffer();
		}
		if (!bb.hasRemaining()) {
			bufferProvider.addBuffer(bb);
			bb = bufferProvider.takeEmptyBuffer();
		}
		if (bb.remaining() < len) {
			final int remaining = bb.remaining();
			bb.put(b, off, remaining);
			if (debug)
				logger.log(Level.INFO, "writing byte[] " + remaining);
			bufferProvider.addBuffer(bb);
			bb = bufferProvider.takeEmptyBuffer();
			write(b, off + remaining, len - remaining);
		} else {
			bb.put(b, off, len);
			if (debug)
				logger.log(Level.INFO, "writing byte[] " + len);
		}
	}

	@Override
	public void write(final int b) {
		if (bb == null) {
			bb = bufferProvider.takeEmptyBuffer();
		}
		if (!bb.hasRemaining()) {
			bufferProvider.addBuffer(bb);
			bb = bufferProvider.takeEmptyBuffer();
		}
		if (debug)
			logger.log(Level.INFO, "writing byte " + (byte) b);
		bb.put((byte) b);
	}

	public final void writeBoolean(final boolean v) throws IOException {
		write(v ? 1 : 0);
	}

	public final void writeByte(final int v) throws IOException {
		write(v);
	}

	public final void writeBytes(final String s) throws IOException {
		final int len = s.length();
		for (int i = 0; i < len; i++) {
			write((byte) s.charAt(i));
		}
	}

	public final void writeChar(final int v) throws IOException {
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
	}

	public final void writeChars(final String s) throws IOException {
		final int len = s.length();
		for (int i = 0; i < len; i++) {
			final int v = s.charAt(i);
			write((v >>> 8) & 0xFF);
			write((v >>> 0) & 0xFF);
		}
	}

	public final void writeDouble(final double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	public final void writeFloat(final float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	public final void writeInt(final int v) throws IOException {
		write((v >>> 24) & 0xFF);
		write((v >>> 16) & 0xFF);
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
	}

	public final void writeLong(final long v) throws IOException {
		writeBuffer[0] = (byte) (v >>> 56);
		writeBuffer[1] = (byte) (v >>> 48);
		writeBuffer[2] = (byte) (v >>> 40);
		writeBuffer[3] = (byte) (v >>> 32);
		writeBuffer[4] = (byte) (v >>> 24);
		writeBuffer[5] = (byte) (v >>> 16);
		writeBuffer[6] = (byte) (v >>> 8);
		writeBuffer[7] = (byte) (v >>> 0);
		write(writeBuffer, 0, 8);
	}

	public final void writeShort(final int v) throws IOException {
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
	}

	public final void writeUTF(final String str) throws IOException {
		final int strlen = str.length();
		int utflen = 0;
		int c, count = 0;

		/* use charAt instead of copying String to char array */
		for (int i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				utflen++;
			} else if (c > 0x07FF) {
				utflen += 3;
			} else {
				utflen += 2;
			}
		}

		if (utflen > 65535)
			throw new UTFDataFormatException("encoded string too long: " + utflen + " bytes");

		final byte[] bytearr = new byte[utflen + 2];

		bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
		bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);

		int i = 0;
		for (i = 0; i < strlen; i++) {
			c = str.charAt(i);
			if (!((c >= 0x0001) && (c <= 0x007F)))
				break;
			bytearr[count++] = (byte) c;
		}

		for (; i < strlen; i++) {
			c = str.charAt(i);
			if ((c >= 0x0001) && (c <= 0x007F)) {
				bytearr[count++] = (byte) c;

			} else if (c > 0x07FF) {
				bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
				bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
				bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
			} else {
				bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
				bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
			}
		}
		write(bytearr, 0, utflen + 2);
	}
}
