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
package com.hazelcast.internal.json.test.mocking;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.ParseException;
import com.hazelcast.test.annotation.QuickTest;


/**
 * Make sure types do not prevent mocking by final or visibility constructs.
 */
@Category(QuickTest.class)
public class Mocking_Test {

  @Test
  public void mockValue() {
    JsonValue jsonValue = Mockito.mock(JsonValue.class);

    assertNotNull(jsonValue);
  }

  @Test
  public void mockObject() {
    JsonObject jsonObject = Mockito.mock(JsonObject.class);

    assertNotNull(jsonObject);
  }

  @Test
  public void mockArray() {
    JsonArray jsonArray = Mockito.mock(JsonArray.class);

    assertNotNull(jsonArray);
  }

  @Test
  public void mockParseException() {
    Mockito.mock(ParseException.class);
  }

}
