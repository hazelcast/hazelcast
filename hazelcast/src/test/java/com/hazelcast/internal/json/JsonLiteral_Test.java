/*******************************************************************************
 * Copyright (c) 2013, 2015 EclipseSource.
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
package com.hazelcast.internal.json;

import static com.hazelcast.internal.json.Json.FALSE;
import static com.hazelcast.internal.json.Json.NULL;
import static com.hazelcast.internal.json.Json.TRUE;
import static com.hazelcast.test.TestJavaSerializationUtils.serializeAndDeserialize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hazelcast.test.annotation.QuickTest;

import java.io.IOException;

@Category(QuickTest.class)
public class JsonLiteral_Test {

  @Test
  public void isNull() {
    assertTrue(NULL.isNull());

    assertFalse(TRUE.isNull());
    assertFalse(FALSE.isNull());
  }

  @Test
  public void isTrue() {
    assertTrue(TRUE.isTrue());

    assertFalse(NULL.isTrue());
    assertFalse(FALSE.isTrue());
  }

  @Test
  public void isFalse() {
    assertTrue(FALSE.isFalse());

    assertFalse(NULL.isFalse());
    assertFalse(TRUE.isFalse());
  }

  @Test
  public void isBoolean() {
    assertTrue(TRUE.isBoolean());
    assertTrue(FALSE.isBoolean());

    assertFalse(NULL.isBoolean());
  }

  @Test
  public void NULL_write() throws IOException {
    JsonWriter writer = mock(JsonWriter.class);

    NULL.write(writer);

    verify(writer).writeLiteral("null");
    verifyNoMoreInteractions(writer);
  }

  @Test
  public void TRUE_write() throws IOException {
    JsonWriter writer = mock(JsonWriter.class);

    TRUE.write(writer);

    verify(writer).writeLiteral("true");
    verifyNoMoreInteractions(writer);
  }

  @Test
  public void FALSE_write() throws IOException {
    JsonWriter writer = mock(JsonWriter.class);

    FALSE.write(writer);

    verify(writer).writeLiteral("false");
    verifyNoMoreInteractions(writer);
  }

  @Test
  public void NULL_toString() {
    assertEquals("null", NULL.toString());
  }

  @Test
  public void TRUE_toString() {
    assertEquals("true", TRUE.toString());
  }

  @Test
  public void FALSE_toString() {
    assertEquals("false", FALSE.toString());
  }

  @Test
  public void NULL_equals() {
    assertEquals(NULL, NULL);

    assertNotEquals(NULL, null);
    assertNotEquals(NULL, TRUE);
    assertNotEquals(NULL, FALSE);
    assertNotEquals(NULL, Json.value("null"));
  }

  @Test
  public void TRUE_equals() {
    assertEquals(TRUE, TRUE);

    assertNotEquals(TRUE, null);
    assertNotEquals(TRUE, FALSE);
    assertNotEquals(TRUE, Boolean.TRUE);
    assertNotEquals(NULL, Json.value("true"));
  }

  @Test
  public void FALSE_equals() {
    assertEquals(FALSE, FALSE);

    assertNotEquals(FALSE, null);
    assertNotEquals(FALSE, TRUE);
    assertNotEquals(FALSE, Boolean.FALSE);
    assertNotEquals(NULL, Json.value("false"));
  }

  @Test
  public void NULL_isSerializable() throws Exception {
    assertEquals(NULL, serializeAndDeserialize(NULL));
    assertTrue(serializeAndDeserialize(NULL).isNull());
  }

  @Test
  public void TRUE_isSerializable() throws Exception {
    assertEquals(TRUE, serializeAndDeserialize(TRUE));
    assertTrue(serializeAndDeserialize(TRUE).isBoolean());
    assertTrue(serializeAndDeserialize(TRUE).isTrue());
  }

  @Test
  public void FALSE_isSerializable() throws Exception {
    assertEquals(FALSE, serializeAndDeserialize(FALSE));
    assertTrue(serializeAndDeserialize(FALSE).isBoolean());
    assertTrue(serializeAndDeserialize(FALSE).isFalse());
  }

  @Test
  public void sameAfterDeserialization() throws Exception {
    JsonArray array = new JsonArray().add(NULL).add(NULL);

    JsonArray deserialized = serializeAndDeserialize(array);

    assertNotSame(NULL, deserialized.get(0));
    assertSame(deserialized.get(0), deserialized.get(1));
  }

}
