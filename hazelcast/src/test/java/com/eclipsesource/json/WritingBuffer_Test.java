/*******************************************************************************
 * Copyright (c) 2015 EclipseSource.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 ******************************************************************************/
package com.eclipsesource.json;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;


public class WritingBuffer_Test {

  private static final int BUFFER_SIZE = 16;
  private StringWriter wrapped;
  private WritingBuffer writer;

  @Before
  public void setUp() {
    wrapped = new StringWriter();
    writer = new WritingBuffer(wrapped, BUFFER_SIZE);
  }

  @Test
  public void testFlushEmpty() throws IOException {
    writer.flush();

    assertEquals("", wrapped.toString());
  }

  @Test
  public void testWriteChar() throws IOException {
    writer.write('c');
    writer.flush();

    assertEquals("c", wrapped.toString());
  }

  @Test
  public void testWriteChar_fit() throws IOException {
    writer.write(createString(BUFFER_SIZE - 1));
    writer.write('c');
    writer.flush();

    assertEquals(createString(BUFFER_SIZE - 1) + "c", wrapped.toString());
  }

  @Test
  public void testWriteChar_exceeding() throws IOException {
    writer.write(createString(BUFFER_SIZE));
    writer.write('c');
    writer.flush();

    assertEquals(createString(BUFFER_SIZE) + "c", wrapped.toString());
  }

  @Test
  public void testWriteCharArray() throws IOException {
    writer.write("foobar".toCharArray(), 1, 3);
    writer.flush();

    assertEquals("oob", wrapped.toString());
  }

  @Test
  public void testWriteCharArray_fit() throws IOException {
    writer.write(createString(BUFFER_SIZE - 3));
    writer.write("foobar".toCharArray(), 1, 3);
    writer.flush();

    assertEquals(createString(BUFFER_SIZE - 3) + "oob", wrapped.toString());
  }

  @Test
  public void testWriteCharArray_exceeding() throws IOException {
    writer.write(createString(BUFFER_SIZE - 2));
    writer.write("foobar".toCharArray(), 1, 3);
    writer.flush();

    assertEquals(createString(BUFFER_SIZE - 2) + "oob", wrapped.toString());
  }

  @Test
  public void testWriteCharArray_exceedingBuffer() throws IOException {
    writer.write(createChars(BUFFER_SIZE + 1));
    writer.flush();

    assertEquals(createString(BUFFER_SIZE + 1), wrapped.toString());
  }

  @Test
  public void testWriteString() throws IOException {
    writer.write("foobar", 1, 3);
    writer.flush();

    assertEquals("oob", wrapped.toString());
  }

  @Test
  public void testWriteString_fit() throws IOException {
    writer.write(createString(BUFFER_SIZE - 3));
    writer.write("foobar", 1, 3);
    writer.flush();

    assertEquals(createString(BUFFER_SIZE - 3) + "oob", wrapped.toString());
  }

  @Test
  public void testWriteString_exceeding() throws IOException {
    writer.write(createString(BUFFER_SIZE - 2));
    writer.write("foobar", 1, 3);
    writer.flush();

    assertEquals(createString(BUFFER_SIZE - 2) + "oob", wrapped.toString());
  }

  @Test
  public void testWriteString_exceedingBuffer() throws IOException {
    writer.write(createString(BUFFER_SIZE + 1));
    writer.flush();

    assertEquals(createString(BUFFER_SIZE + 1), wrapped.toString());
  }

  private static String createString(int length) {
    return new String(createChars(length));
  }

  private static char[] createChars(int length) {
    char[] array = new char[length];
    Arrays.fill(array, 'x');
    return array;
  }

}
