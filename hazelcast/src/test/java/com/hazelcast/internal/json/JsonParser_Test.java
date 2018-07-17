/*******************************************************************************
 * Copyright (c) 2013, 2016 EclipseSource.
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

import static com.hazelcast.internal.json.Json.parse;
import static com.hazelcast.internal.json.TestUtil.assertException;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonHandler;
import com.hazelcast.internal.json.JsonNumber;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.json.JsonParser;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.json.Location;
import com.hazelcast.internal.json.ParseException;
import com.hazelcast.internal.json.Json.DefaultHandler;
import com.hazelcast.internal.json.TestUtil.RunnableEx;
import com.hazelcast.test.annotation.QuickTest;

@Category(QuickTest.class)
public class JsonParser_Test {

  private TestHandler handler;
  private JsonParser parser;

  @Before
  public void setUp() {
    handler = new TestHandler();
    parser = new JsonParser(handler);
  }

  @Test(expected = NullPointerException.class)
  public void constructor_rejectsNullHandler() {
    new JsonParser(null);
  }

  @Test(expected = NullPointerException.class)
  public void parse_string_rejectsNull() {
    parser.parse((String)null);
  }

  @Test(expected = NullPointerException.class)
  public void parse_reader_rejectsNull() throws IOException {
    parser.parse((Reader)null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void parse_reader_rejectsNegativeBufferSize() throws IOException {
    parser.parse(new StringReader("[]"), -1);
  }

  @Test
  public void parse_string_rejectsEmpty() {
    assertParseException(0, "Unexpected end of input", "");
  }

  @Test
  public void parse_reader_rejectsEmpty() {
    ParseException exception = assertException(ParseException.class, new RunnableEx() {
      public void run() throws IOException {
        parser.parse(new StringReader(""));
      }
    });

    assertEquals(0, exception.getLocation().offset);
    assertThat(exception.getMessage(), startsWith("Unexpected end of input at"));
  }

  @Test
  public void parse_null() {
    parser.parse("null");

    assertEquals(join("startNull 0",
                      "endNull 4"),
                 handler.getLog());
  }

  @Test
  public void parse_true() {
    parser.parse("true");

    assertEquals(join("startBoolean 0",
                      "endBoolean true 4"),
                 handler.getLog());
  }

  @Test
  public void parse_false() {
    parser.parse("false");

    assertEquals(join("startBoolean 0",
                      "endBoolean false 5"),
                 handler.getLog());
  }

  @Test
  public void parse_string() {
    parser.parse("\"foo\"");

    assertEquals(join("startString 0",
                      "endString foo 5"),
                 handler.getLog());
  }

  @Test
  public void parse_string_empty() {
    parser.parse("\"\"");

    assertEquals(join("startString 0",
                      "endString  2"),
                 handler.getLog());
  }

  @Test
  public void parse_number() {
    parser.parse("23");

    assertEquals(join("startNumber 0",
                      "endNumber 23 2"),
                 handler.getLog());
  }

  @Test
  public void parse_number_negative() {
    parser.parse("-23");

    assertEquals(join("startNumber 0",
        "endNumber -23 3"),
                 handler.getLog());
  }

  @Test
  public void parse_number_negative_exponent() {
    parser.parse("-2.3e-12");

    assertEquals(join("startNumber 0",
        "endNumber -2.3e-12 8"),
                 handler.getLog());
  }

  @Test
  public void parse_array() {
    parser.parse("[23]");

    assertEquals(join("startArray 0",
                      "startArrayValue a1 1",
                      "startNumber 1",
                      "endNumber 23 3",
                      "endArrayValue a1 3",
                      "endArray a1 4"),
                 handler.getLog());
  }

  @Test
  public void parse_array_empty() {
    parser.parse("[]");

    assertEquals(join("startArray 0",
                      "endArray a1 2"),
                 handler.getLog());
  }

  @Test
  public void parse_object() {
    parser.parse("{\"foo\": 23}");

    assertEquals(join("startObject 0",
                      "startObjectName o1 1",
                      "endObjectName o1 foo 6",
                      "startObjectValue o1 foo 8",
                      "startNumber 8",
                      "endNumber 23 10",
                      "endObjectValue o1 foo 10",
                      "endObject o1 11"),
                 handler.getLog());
  }

  @Test
  public void parse_object_empty() {
    parser.parse("{}");

    assertEquals(join("startObject 0",
                      "endObject o1 2"),
                 handler.getLog());
  }

  @Test
  public void parse_stripsPadding() {
    assertEquals(new JsonArray(), parse(" [ ] "));
  }

  @Test
  public void parse_ignoresAllWhiteSpace() {
    assertEquals(new JsonArray(), parse("\t\r\n [\t\r\n ]\t\r\n "));
  }

  @Test
  public void parse_failsWithUnterminatedString() {
    assertParseException(5, "Unexpected end of input", "[\"foo");
  }

  @Test
  public void parse_lineAndColumn_onFirstLine() {
    parser.parse("[]");

    assertEquals("1:3", handler.lastLocation.toString());
  }

  @Test
  public void parse_lineAndColumn_afterLF() {
    parser.parse("[\n]");

    assertEquals("2:2", handler.lastLocation.toString());
  }

  @Test
  public void parse_lineAndColumn_afterCRLF() {
    parser.parse("[\r\n]");

    assertEquals("2:2", handler.lastLocation.toString());
  }

  @Test
  public void parse_lineAndColumn_afterCR() {
    parser.parse("[\r]");

    assertEquals("1:4", handler.lastLocation.toString());
  }

  @Test
  public void parse_handlesInputsThatExceedBufferSize() throws IOException {
    DefaultHandler defHandler = new DefaultHandler();
    parser = new JsonParser(defHandler);
    String input = "[ 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47 ]";

    parser.parse(new StringReader(input), 3);

    assertEquals("[2,3,5,7,11,13,17,19,23,29,31,37,41,43,47]", defHandler.getValue().toString());
  }

  @Test
  public void parse_handlesStringsThatExceedBufferSize() throws IOException {
    DefaultHandler defHandler = new DefaultHandler();
    parser = new JsonParser(defHandler);
    String input = "[ \"lorem ipsum dolor sit amet\" ]";

    parser.parse(new StringReader(input), 3);

    assertEquals("[\"lorem ipsum dolor sit amet\"]", defHandler.getValue().toString());
  }

  @Test
  public void parse_handlesNumbersThatExceedBufferSize() throws IOException {
    DefaultHandler defHandler = new DefaultHandler();
    parser = new JsonParser(defHandler);
    String input = "[ 3.141592653589 ]";

    parser.parse(new StringReader(input), 3);

    assertEquals("[3.141592653589]", defHandler.getValue().toString());
  }

  @Test
  public void parse_handlesPositionsCorrectlyWhenInputExceedsBufferSize() {
    final String input = "{\n  \"a\": 23,\n  \"b\": 42,\n}";

    ParseException exception = assertException(ParseException.class, new RunnableEx() {
      public void run() throws IOException {
        parser.parse(new StringReader(input), 3);
      }
    });

    assertEquals(new Location(24, 4, 1), exception.getLocation());
  }

  @Test
  public void parse_failsOnTooDeeplyNestedArray() {
    JsonArray array = new JsonArray();
    for (int i = 0; i < 1001; i++) {
      array = new JsonArray().add(array);
    }
    final String input = array.toString();

    ParseException exception = assertException(ParseException.class, new RunnableEx() {
      public void run() throws IOException {
        parser.parse(input);
      }
    });

    assertEquals("Nesting too deep at 1:1002", exception.getMessage());
  }

  @Test
  public void parse_failsOnTooDeeplyNestedObject() {
    JsonObject object = new JsonObject();
    for (int i = 0; i < 1001; i++) {
      object = new JsonObject().add("foo", object);
    }
    final String input = object.toString();

    ParseException exception = assertException(ParseException.class, new RunnableEx() {
      public void run() throws IOException {
        parser.parse(input);
      }
    });

    assertEquals("Nesting too deep at 1:7002", exception.getMessage());
  }

  @Test
  public void parse_failsOnTooDeeplyNestedMixedObject() {
    JsonValue value = new JsonObject();
    for (int i = 0; i < 1001; i++) {
      value = i % 2 == 0 ? new JsonArray().add(value) : new JsonObject().add("foo", value);
    }
    final String input = value.toString();

    ParseException exception = assertException(ParseException.class, new RunnableEx() {
      public void run() throws IOException {
        parser.parse(input);
      }
    });

    assertEquals("Nesting too deep at 1:4002", exception.getMessage());
  }

  @Test
  public void parse_doesNotFailWithManyArrays() {
    JsonArray array = new JsonArray();
    for (int i = 0; i < 1001; i++) {
      array.add(new JsonArray().add(7));
    }
    final String input = array.toString();

    JsonValue result = parse(input);

    assertTrue(result.isArray());
  }

  @Test
  public void parse_doesNotFailWithManyEmptyArrays() {
    JsonArray array = new JsonArray();
    for (int i = 0; i < 1001; i++) {
      array.add(new JsonArray());
    }
    final String input = array.toString();

    JsonValue result = parse(input);

    assertTrue(result.isArray());
  }

  @Test
  public void parse_doesNotFailWithManyObjects() {
    JsonArray array = new JsonArray();
    for (int i = 0; i < 1001; i++) {
      array.add(new JsonObject().add("a", 7));
    }
    final String input = array.toString();

    JsonValue result = parse(input);

    assertTrue(result.isArray());
  }

  @Test
  public void parse_doesNotFailWithManyEmptyObjects() {
    JsonArray array = new JsonArray();
    for (int i = 0; i < 1001; i++) {
      array.add(new JsonObject());
    }
    final String input = array.toString();

    JsonValue result = parse(input);

    assertTrue(result.isArray());
  }

  @Test
  public void parse_canBeCalledTwice() {
    parser.parse("[23]");

    parser.parse("[42]");

    assertEquals(join(// first run
                      "startArray 0",
                      "startArrayValue a1 1",
                      "startNumber 1",
                      "endNumber 23 3",
                      "endArrayValue a1 3",
                      "endArray a1 4",
                      // second run
                      "startArray 0",
                      "startArrayValue a2 1",
                      "startNumber 1",
                      "endNumber 42 3",
                      "endArrayValue a2 3",
                      "endArray a2 4"),
                 handler.getLog());
  }

  @Test
  public void arrays_empty() {
    assertEquals("[]", parse("[]").toString());
  }

  @Test
  public void arrays_singleValue() {
    assertEquals("[23]", parse("[23]").toString());
  }

  @Test
  public void arrays_multipleValues() {
    assertEquals("[23,42]", parse("[23,42]").toString());
  }

  @Test
  public void arrays_withWhitespaces() {
    assertEquals("[23,42]", parse("[ 23 , 42 ]").toString());
  }

  @Test
  public void arrays_nested() {
    assertEquals("[[23]]", parse("[[23]]").toString());
    assertEquals("[[[]]]", parse("[[[]]]").toString());
    assertEquals("[[23],42]", parse("[[23],42]").toString());
    assertEquals("[[23],[42]]", parse("[[23],[42]]").toString());
    assertEquals("[[23],[42]]", parse("[[23],[42]]").toString());
    assertEquals("[{\"foo\":[23]},{\"bar\":[42]}]",
                 parse("[{\"foo\":[23]},{\"bar\":[42]}]").toString());
  }

  @Test
  public void arrays_illegalSyntax() {
    assertParseException(1, "Expected value", "[,]");
    assertParseException(4, "Expected ',' or ']'", "[23 42]");
    assertParseException(4, "Expected value", "[23,]");
  }

  @Test
  public void arrays_incomplete() {
    assertParseException(1, "Unexpected end of input", "[");
    assertParseException(2, "Unexpected end of input", "[ ");
    assertParseException(3, "Unexpected end of input", "[23");
    assertParseException(4, "Unexpected end of input", "[23 ");
    assertParseException(4, "Unexpected end of input", "[23,");
    assertParseException(5, "Unexpected end of input", "[23, ");
  }

  @Test
  public void objects_empty() {
    assertEquals("{}", parse("{}").toString());
  }

  @Test
  public void objects_singleValue() {
    assertEquals("{\"foo\":23}", parse("{\"foo\":23}").toString());
  }

  @Test
  public void objects_multipleValues() {
    assertEquals("{\"foo\":23,\"bar\":42}", parse("{\"foo\":23,\"bar\":42}").toString());
  }

  @Test
  public void objects_whitespace() {
    assertEquals("{\"foo\":23,\"bar\":42}", parse("{ \"foo\" : 23, \"bar\" : 42 }").toString());
  }

  @Test
  public void objects_nested() {
    assertEquals("{\"foo\":{}}", parse("{\"foo\":{}}").toString());
    assertEquals("{\"foo\":{\"bar\":42}}", parse("{\"foo\":{\"bar\": 42}}").toString());
    assertEquals("{\"foo\":{\"bar\":{\"baz\":42}}}",
                 parse("{\"foo\":{\"bar\": {\"baz\": 42}}}").toString());
    assertEquals("{\"foo\":[{\"bar\":{\"baz\":[[42]]}}]}",
                 parse("{\"foo\":[{\"bar\": {\"baz\": [[42]]}}]}").toString());
  }

  @Test
  public void objects_illegalSyntax() {
    assertParseException(1, "Expected name", "{,}");
    assertParseException(1, "Expected name", "{:}");
    assertParseException(1, "Expected name", "{23}");
    assertParseException(4, "Expected ':'", "{\"a\"}");
    assertParseException(5, "Expected ':'", "{\"a\" \"b\"}");
    assertParseException(5, "Expected value", "{\"a\":}");
    assertParseException(8, "Expected name", "{\"a\":23,}");
    assertParseException(8, "Expected name", "{\"a\":23,42");
  }

  @Test
  public void objects_incomplete() {
    assertParseException(1, "Unexpected end of input", "{");
    assertParseException(2, "Unexpected end of input", "{ ");
    assertParseException(2, "Unexpected end of input", "{\"");
    assertParseException(4, "Unexpected end of input", "{\"a\"");
    assertParseException(5, "Unexpected end of input", "{\"a\" ");
    assertParseException(5, "Unexpected end of input", "{\"a\":");
    assertParseException(6, "Unexpected end of input", "{\"a\": ");
    assertParseException(7, "Unexpected end of input", "{\"a\":23");
    assertParseException(8, "Unexpected end of input", "{\"a\":23 ");
    assertParseException(8, "Unexpected end of input", "{\"a\":23,");
    assertParseException(9, "Unexpected end of input", "{\"a\":23, ");
  }

  @Test
  public void strings_emptyString_isAccepted() {
    assertEquals("", parse("\"\"").asString());
  }

  @Test
  public void strings_asciiCharacters_areAccepted() {
    assertEquals(" ", parse("\" \"").asString());
    assertEquals("a", parse("\"a\"").asString());
    assertEquals("foo", parse("\"foo\"").asString());
    assertEquals("A2-D2", parse("\"A2-D2\"").asString());
    assertEquals("\u007f", parse("\"\u007f\"").asString());
  }

  @Test
  public void strings_nonAsciiCharacters_areAccepted() {
    assertEquals("Русский", parse("\"Русский\"").asString());
    assertEquals("العربية", parse("\"العربية\"").asString());
    assertEquals("日本語", parse("\"日本語\"").asString());
  }

  @Test
  public void strings_controlCharacters_areRejected() {
    // JSON string must not contain characters < 0x20
    assertParseException(3, "Expected valid string character", "\"--\n--\"");
    assertParseException(3, "Expected valid string character", "\"--\r\n--\"");
    assertParseException(3, "Expected valid string character", "\"--\t--\"");
    assertParseException(3, "Expected valid string character", "\"--\u0000--\"");
    assertParseException(3, "Expected valid string character", "\"--\u001f--\"");
  }

  @Test
  public void strings_validEscapes_areAccepted() {
    // valid escapes are \" \\ \/ \b \f \n \r \t and unicode escapes
    assertEquals(" \" ", parse("\" \\\" \"").asString());
    assertEquals(" \\ ", parse("\" \\\\ \"").asString());
    assertEquals(" / ", parse("\" \\/ \"").asString());
    assertEquals(" \u0008 ", parse("\" \\b \"").asString());
    assertEquals(" \u000c ", parse("\" \\f \"").asString());
    assertEquals(" \r ", parse("\" \\r \"").asString());
    assertEquals(" \n ", parse("\" \\n \"").asString());
    assertEquals(" \t ", parse("\" \\t \"").asString());
  }

  @Test
  public void strings_escape_atStart() {
    assertEquals("\\x", parse("\"\\\\x\"").asString());
  }

  @Test
  public void strings_escape_atEnd() {
    assertEquals("x\\", parse("\"x\\\\\"").asString());
  }

  @Test
  public void strings_illegalEscapes_areRejected() {
    assertParseException(2, "Expected valid escape sequence", "\"\\a\"");
    assertParseException(2, "Expected valid escape sequence", "\"\\x\"");
    assertParseException(2, "Expected valid escape sequence", "\"\\000\"");
  }

  @Test
  public void strings_validUnicodeEscapes_areAccepted() {
    assertEquals("\u0021", parse("\"\\u0021\"").asString());
    assertEquals("\u4711", parse("\"\\u4711\"").asString());
    assertEquals("\uffff", parse("\"\\uffff\"").asString());
    assertEquals("\uabcdx", parse("\"\\uabcdx\"").asString());
  }

  @Test
  public void strings_illegalUnicodeEscapes_areRejected() {
    assertParseException(3, "Expected hexadecimal digit", "\"\\u \"");
    assertParseException(3, "Expected hexadecimal digit", "\"\\ux\"");
    assertParseException(5, "Expected hexadecimal digit", "\"\\u20 \"");
    assertParseException(6, "Expected hexadecimal digit", "\"\\u000x\"");
  }

  @Test
  public void strings_incompleteStrings_areRejected() {
    assertParseException(1, "Unexpected end of input", "\"");
    assertParseException(4, "Unexpected end of input", "\"foo");
    assertParseException(5, "Unexpected end of input", "\"foo\\");
    assertParseException(6, "Unexpected end of input", "\"foo\\n");
    assertParseException(6, "Unexpected end of input", "\"foo\\u");
    assertParseException(7, "Unexpected end of input", "\"foo\\u0");
    assertParseException(9, "Unexpected end of input", "\"foo\\u000");
    assertParseException(10, "Unexpected end of input", "\"foo\\u0000");
  }

  @Test
  public void numbers_integer() {
    assertEquals(new JsonNumber("0"), parse("0"));
    assertEquals(new JsonNumber("-0"), parse("-0"));
    assertEquals(new JsonNumber("1"), parse("1"));
    assertEquals(new JsonNumber("-1"), parse("-1"));
    assertEquals(new JsonNumber("23"), parse("23"));
    assertEquals(new JsonNumber("-23"), parse("-23"));
    assertEquals(new JsonNumber("1234567890"), parse("1234567890"));
    assertEquals(new JsonNumber("123456789012345678901234567890"),
                 parse("123456789012345678901234567890"));
  }

  @Test
  public void numbers_minusZero() {
    // allowed by JSON, allowed by Java
    JsonValue value = parse("-0");

    assertEquals(0, value.asInt());
    assertEquals(0l, value.asLong());
    assertEquals(0f, value.asFloat(), 0);
    assertEquals(0d, value.asDouble(), 0);
  }

  @Test
  public void numbers_decimal() {
    assertEquals(new JsonNumber("0.23"), parse("0.23"));
    assertEquals(new JsonNumber("-0.23"), parse("-0.23"));
    assertEquals(new JsonNumber("1234567890.12345678901234567890"),
                 parse("1234567890.12345678901234567890"));
  }

  @Test
  public void numbers_withExponent() {
    assertEquals(new JsonNumber("0.1e9"), parse("0.1e9"));
    assertEquals(new JsonNumber("0.1e9"), parse("0.1e9"));
    assertEquals(new JsonNumber("0.1E9"), parse("0.1E9"));
    assertEquals(new JsonNumber("-0.23e9"), parse("-0.23e9"));
    assertEquals(new JsonNumber("0.23e9"), parse("0.23e9"));
    assertEquals(new JsonNumber("0.23e+9"), parse("0.23e+9"));
    assertEquals(new JsonNumber("0.23e-9"), parse("0.23e-9"));
  }

  @Test
  public void numbers_withInvalidFormat() {
    assertParseException(0, "Expected value", "+1");
    assertParseException(0, "Expected value", ".1");
    assertParseException(1, "Unexpected character", "02");
    assertParseException(2, "Unexpected character", "-02");
    assertParseException(1, "Expected digit", "-x");
    assertParseException(2, "Expected digit", "1.x");
    assertParseException(2, "Expected digit", "1ex");
    assertParseException(3, "Unexpected character", "1e1x");
  }

  @Test
  public void numbers_incomplete() {
    assertParseException(1, "Unexpected end of input", "-");
    assertParseException(2, "Unexpected end of input", "1.");
    assertParseException(4, "Unexpected end of input", "1.0e");
    assertParseException(5, "Unexpected end of input", "1.0e-");
  }

  @Test
  public void null_complete() {
    assertEquals(Json.NULL, parse("null"));
  }

  @Test
  public void null_incomplete() {
    assertParseException(1, "Unexpected end of input", "n");
    assertParseException(2, "Unexpected end of input", "nu");
    assertParseException(3, "Unexpected end of input", "nul");
  }

  @Test
  public void null_withIllegalCharacter() {
    assertParseException(1, "Expected 'u'", "nx");
    assertParseException(2, "Expected 'l'", "nux");
    assertParseException(3, "Expected 'l'", "nulx");
    assertParseException(4, "Unexpected character", "nullx");
  }

  @Test
  public void true_complete() {
    assertSame(Json.TRUE, parse("true"));
  }

  @Test
  public void true_incomplete() {
    assertParseException(1, "Unexpected end of input", "t");
    assertParseException(2, "Unexpected end of input", "tr");
    assertParseException(3, "Unexpected end of input", "tru");
  }

  @Test
  public void true_withIllegalCharacter() {
    assertParseException(1, "Expected 'r'", "tx");
    assertParseException(2, "Expected 'u'", "trx");
    assertParseException(3, "Expected 'e'", "trux");
    assertParseException(4, "Unexpected character", "truex");
  }

  @Test
  public void false_complete() {
    assertSame(Json.FALSE, parse("false"));
  }

  @Test
  public void false_incomplete() {
    assertParseException(1, "Unexpected end of input", "f");
    assertParseException(2, "Unexpected end of input", "fa");
    assertParseException(3, "Unexpected end of input", "fal");
    assertParseException(4, "Unexpected end of input", "fals");
  }

  @Test
  public void false_withIllegalCharacter() {
    assertParseException(1, "Expected 'a'", "fx");
    assertParseException(2, "Expected 'l'", "fax");
    assertParseException(3, "Expected 's'", "falx");
    assertParseException(4, "Expected 'e'", "falsx");
    assertParseException(5, "Unexpected character", "falsex");
  }

  private void assertParseException(int offset, String message, final String json) {
    ParseException exception = assertException(ParseException.class, new Runnable() {
      public void run() {
        parser.parse(json);
      }
    });
    assertEquals(offset, exception.getLocation().offset);
    assertThat(exception.getMessage(), startsWith(message + " at"));
  }

  private static String join(String... strings) {
    StringBuilder builder = new StringBuilder();
    for (String string : strings) {
      builder.append(string).append('\n');
    }
    return builder.toString();
  }

  static class TestHandler extends JsonHandler<Object, Object> {

    Location lastLocation;
    StringBuilder log = new StringBuilder();
    int sequence = 0;

    @Override
    public void startNull() {
      record("startNull");
    }

    @Override
    public void endNull() {
      record("endNull");
    }

    @Override
    public void startBoolean() {
      record("startBoolean");
    }

    @Override
    public void endBoolean(boolean value) {
      record("endBoolean", Boolean.valueOf(value));
    }

    @Override
    public void startString() {
      record("startString");
    }

    @Override
    public void endString(String string) {
      record("endString", string);
    }

    @Override
    public void startNumber() {
      record("startNumber");
    }

    @Override
    public void endNumber(String string) {
      record("endNumber", string);
    }

    @Override
    public Object startArray() {
      record("startArray");
      return "a" + ++sequence;
    }

    @Override
    public void endArray(Object array) {
      record("endArray", array);
    }

    @Override
    public void startArrayValue(Object array) {
      record("startArrayValue", array);
    }

    @Override
    public void endArrayValue(Object array) {
      record("endArrayValue", array);
    }

    @Override
    public Object startObject() {
      record("startObject");
      return "o" + ++sequence;
    }

    @Override
    public void endObject(Object object) {
      record("endObject", object);
    }

    @Override
    public void startObjectName(Object object) {
      record("startObjectName", object);
    }

    @Override
    public void endObjectName(Object object, String name) {
      record("endObjectName", object, name);
    }

    @Override
    public void startObjectValue(Object object, String name) {
      record("startObjectValue", object, name);
    }

    @Override
    public void endObjectValue(Object object, String name) {
      record("endObjectValue", object, name);
    }

    private void record(String event, Object... args) {
      lastLocation = getLocation();
      log.append(event);
      for (Object arg : args) {
        log.append(' ').append(arg);
      }
      log.append(' ').append(lastLocation.offset).append('\n');
    }

    String getLog() {
      return log.toString();
    }

  }

}
