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

public class BuffersOutputStream extends OutputStream implements DataOutput {

	ByteBuffer bb = null;

	BufferProvider bufferProvider = null;

	boolean first = false;

	private static final boolean debug = false;

	public BuffersOutputStream() {
	}

	public void setBufferProvider(BufferProvider bufferProvider) {
		this.bufferProvider = bufferProvider;
	}

	@Override
	public void write(int b) {
		if (bb == null) {
			bb = bufferProvider.takeEmptyBuffer();
		}
		if (!bb.hasRemaining()) {
			bufferProvider.addBuffer(bb);
			bb = bufferProvider.takeEmptyBuffer();
		}
		if (debug)
			System.out.println("writing byte " + (byte) b);
		bb.put((byte) b);
	}

	@Override
	public void write(byte b[], int off, int len) {
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
			int remaining = bb.remaining();
			bb.put(b, off, remaining);
			if (debug)
				System.out.println("writing byte[] " + remaining);
			bufferProvider.addBuffer(bb);
			bb = bufferProvider.takeEmptyBuffer();
			write(b, off + remaining, len - remaining);
		} else {
			bb.put(b, off, len);
			if (debug)
				System.out.println("writing byte[] " + len);
		}
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

	/**
	 * Writes a <code>boolean</code> to the underlying output stream as a 1-byte
	 * value. The value <code>true</code> is written out as the value
	 * <code>(byte)1</code>; the value <code>false</code> is written out as the
	 * value <code>(byte)0</code>. If no exception is thrown, the counter
	 * <code>written</code> is incremented by <code>1</code>.
	 * 
	 * @param v
	 *            a <code>boolean</code> value to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeBoolean(boolean v) throws IOException {
		write(v ? 1 : 0);
	}

	/**
	 * Writes out a <code>byte</code> to the underlying output stream as a
	 * 1-byte value. If no exception is thrown, the counter <code>written</code>
	 * is incremented by <code>1</code>.
	 * 
	 * @param v
	 *            a <code>byte</code> value to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeByte(int v) throws IOException {
		write(v);
	}

	/**
	 * Writes a <code>short</code> to the underlying output stream as two bytes,
	 * high byte first. If no exception is thrown, the counter
	 * <code>written</code> is incremented by <code>2</code>.
	 * 
	 * @param v
	 *            a <code>short</code> to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeShort(int v) throws IOException {
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
	}

	/**
	 * Writes a <code>char</code> to the underlying output stream as a 2-byte
	 * value, high byte first. If no exception is thrown, the counter
	 * <code>written</code> is incremented by <code>2</code>.
	 * 
	 * @param v
	 *            a <code>char</code> value to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeChar(int v) throws IOException {
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
	}

	/**
	 * Writes an <code>int</code> to the underlying output stream as four bytes,
	 * high byte first. If no exception is thrown, the counter
	 * <code>written</code> is incremented by <code>4</code>.
	 * 
	 * @param v
	 *            an <code>int</code> to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeInt(int v) throws IOException {
		write((v >>> 24) & 0xFF);
		write((v >>> 16) & 0xFF);
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
	}

	private byte writeBuffer[] = new byte[8];

	/**
	 * Writes a <code>long</code> to the underlying output stream as eight
	 * bytes, high byte first. In no exception is thrown, the counter
	 * <code>written</code> is incremented by <code>8</code>.
	 * 
	 * @param v
	 *            a <code>long</code> to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeLong(long v) throws IOException {
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

	/**
	 * Converts the float argument to an <code>int</code> using the
	 * <code>floatToIntBits</code> method in class <code>Float</code>, and then
	 * writes that <code>int</code> value to the underlying output stream as a
	 * 4-byte quantity, high byte first. If no exception is thrown, the counter
	 * <code>written</code> is incremented by <code>4</code>.
	 * 
	 * @param v
	 *            a <code>float</code> value to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 * @see java.lang.Float#floatToIntBits(float)
	 */
	public final void writeFloat(float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	/**
	 * Converts the double argument to a <code>long</code> using the
	 * <code>doubleToLongBits</code> method in class <code>Double</code>, and
	 * then writes that <code>long</code> value to the underlying output stream
	 * as an 8-byte quantity, high byte first. If no exception is thrown, the
	 * counter <code>written</code> is incremented by <code>8</code>.
	 * 
	 * @param v
	 *            a <code>double</code> value to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 * @see java.lang.Double#doubleToLongBits(double)
	 */
	public final void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	/**
	 * Writes out the string to the underlying output stream as a sequence of
	 * bytes. Each character in the string is written out, in sequence, by
	 * discarding its high eight bits. If no exception is thrown, the counter
	 * <code>written</code> is incremented by the length of <code>s</code>.
	 * 
	 * @param s
	 *            a string of bytes to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeBytes(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; i++) {
			write((byte) s.charAt(i));
		}
	}

	/**
	 * Writes a string to the underlying output stream as a sequence of
	 * characters. Each character is written to the data output stream as if by
	 * the <code>writeChar</code> method. If no exception is thrown, the counter
	 * <code>written</code> is incremented by twice the length of <code>s</code>
	 * .
	 * 
	 * @param s
	 *            a <code>String</code> value to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 * @see java.io.DataOutputStream#writeChar(int)
	 * @see java.io.FilterOutputStream#out
	 */
	public final void writeChars(String s) throws IOException {
		int len = s.length();
		for (int i = 0; i < len; i++) {
			int v = s.charAt(i);
			write((v >>> 8) & 0xFF);
			write((v >>> 0) & 0xFF);
		}
	}

	/**
	 * Writes a string to the underlying output stream using <a
	 * href="DataInput.html#modified-utf-8">modified UTF-8</a> encoding in a
	 * machine-independent manner.
	 * <p>
	 * First, two bytes are written to the output stream as if by the
	 * <code>writeShort</code> method giving the number of bytes to follow. This
	 * value is the number of bytes actually written out, not the length of the
	 * string. Following the length, each character of the string is output, in
	 * sequence, using the modified UTF-8 encoding for the character. If no
	 * exception is thrown, the counter <code>written</code> is incremented by
	 * the total number of bytes written to the output stream. This will be at
	 * least two plus the length of <code>str</code>, and at most two plus
	 * thrice the length of <code>str</code>.
	 * 
	 * @param str
	 *            a string to be written.
	 * @exception IOException
	 *                if an I/O error occurs.
	 */
	public final void writeUTF(String str) throws IOException {
		int strlen = str.length();
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

		byte[] bytearr = new byte[utflen + 2];

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
