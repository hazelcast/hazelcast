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

import static com.hazelcast.internal.json.TestUtil.assertException;
import static com.hazelcast.internal.json.TestUtil.serializeAndDeserialize;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.StringWriter;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hazelcast.internal.json.JsonString;
import com.hazelcast.internal.json.JsonWriter;
import com.hazelcast.test.annotation.QuickTest;

@Category(QuickTest.class)
public class JsonString_Test {

  private StringWriter stringWriter;
  private JsonWriter jsonWriter;

  @Before
  public void setUp() {
    stringWriter = new StringWriter();
    jsonWriter = new JsonWriter(stringWriter);
  }

  @Test
  public void constructor_failsWithNull() {
    assertException(NullPointerException.class, "string is null", new Runnable() {
      public void run() {
        new JsonString(null);
      }
    });
  }

  @Test
  public void write() throws IOException {
    new JsonString("foo").write(jsonWriter);

    assertEquals("\"foo\"", stringWriter.toString());
  }

  @Test
  public void write_escapesStrings() throws IOException {
    new JsonString("foo\\bar").write(jsonWriter);

    assertEquals("\"foo\\\\bar\"", stringWriter.toString());
  }

  @Test
  public void isString() {
    assertTrue(new JsonString("foo").isString());
  }

  @Test
  public void asString() {
    assertEquals("foo", new JsonString("foo").asString());
  }

  @Test
  public void equals_trueForSameInstance() {
    JsonString string = new JsonString("foo");

    assertTrue(string.equals(string));
  }

  @Test
  public void equals_trueForEqualStrings() {
    assertTrue(new JsonString("foo").equals(new JsonString("foo")));
  }

  @Test
  public void equals_falseForDifferentStrings() {
    assertFalse(new JsonString("").equals(new JsonString("foo")));
    assertFalse(new JsonString("foo").equals(new JsonString("bar")));
  }

  @Test
  public void equals_falseForNull() {
    assertFalse(new JsonString("foo").equals(null));
  }

  @Test
  public void equals_falseForSubclass() {
    assertFalse(new JsonString("foo").equals(new JsonString("foo") {}));
  }

  @Test
  public void hashCode_equalsForEqualStrings() {
    assertTrue(new JsonString("foo").hashCode() == new JsonString("foo").hashCode());
  }

  @Test
  public void hashCode_differsForDifferentStrings() {
    assertFalse(new JsonString("").hashCode() == new JsonString("foo").hashCode());
    assertFalse(new JsonString("foo").hashCode() == new JsonString("bar").hashCode());
  }

  @Test
  public void canBeSerializedAndDeserialized() throws Exception {
    JsonString string = new JsonString("foo");

    assertEquals(string, serializeAndDeserialize(string));
  }

}
