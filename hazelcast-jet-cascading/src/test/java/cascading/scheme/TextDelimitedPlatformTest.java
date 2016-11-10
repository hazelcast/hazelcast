/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.scheme;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import cascading.ComparePlatformsTest;
import cascading.PlatformTestCase;
import cascading.TestConstants;
import cascading.flow.Flow;
import cascading.operation.AssertionLevel;
import cascading.operation.assertion.AssertExpression;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.MultiSinkTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryIterator;
import cascading.tuple.type.DateType;
import org.junit.Ignore;
import org.junit.Test;

import static data.InputData.*;

/**
 *
 */
public class TextDelimitedPlatformTest extends PlatformTestCase
  {
  public TextDelimitedPlatformTest()
    {
    }

  @Test
  public void testQuotedText() throws IOException
    {
    runQuotedText( "normchar", testDelimited, ",", false );
    }

  @Test
  public void testQuotedTextAll() throws IOException
    {
    runQuotedText( "normchar", testDelimited, ",", true );
    }

  @Test
  public void testQuotedTextSpecChar() throws IOException
    {
    runQuotedText( "specchar", testDelimitedSpecialCharData, "|", false );
    }

  @Test
  public void testQuotedTextSpecCharAll() throws IOException
    {
    runQuotedText( "specchar", testDelimitedSpecialCharData, "|", true );
    }

  private void runQuotedText( String path, String inputData, String delimiter, boolean useAll ) throws IOException
    {

    copyFromLocal(inputData);

    Object[][] results = new Object[][]{
      {"foo", "bar", "baz", "bin", 1L},
      {"foo", "bar", "baz", "bin", 2L},
      {"foo", "bar" + delimiter + "bar", "baz", "bin", 3L},
      {"foo", "bar\"" + delimiter + "bar", "baz", "bin", 4L},
      {"foo", "bar\"\"" + delimiter + "bar", "baz", "bin", 5L},
      {null, null, "baz", null, 6L},
      {null, null, null, null, 7L},
      {"foo", null, null, null, 8L},
      {null, null, null, null, 9L},
      {"f", null, null, null, 10L}, // this one is quoted, single char
      {"f", null, null, ",bin", 11L},
      {"f", null, null, "bin,", 11L}
    };

    if( useAll )
      {
      for( int i = 0; i < results.length; i++ )
        {
        Object[] result = results[ i ];

        for( int j = 0; j < result.length; j++ )
          result[ j ] = result[ j ] != null ? result[ j ].toString() : null;
        }
      }

    Tuple[] tuples = new Tuple[ results.length ];

    for( int i = 0; i < results.length; i++ )
      tuples[ i ] = new Tuple( results[ i ] );

    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    if( useAll )
      {
      types = null;
      fields = Fields.ALL;
      }

    Tap input = getPlatform().getDelimitedFile( fields, false, delimiter, "\"", types, inputData, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, false, delimiter, "\"", types, getOutputPath( "quoted/" + path + "" + useAll ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, results.length, 5 );

    // validate input parsing compares to expected, and results compare to expected
      TupleEntryIterator iterator = flow.openSource();

    int count = 0;
    while (iterator.hasNext())
      {
      Tuple tuple = iterator.next().getTuple();
      assertEquals(tuples[count++], tuple);
      }

    List<Tuple> actual = getSinkAsList(flow);
    Set<Tuple> expected = new HashSet<>();
    Collections.addAll(expected, tuples);

    expected.removeAll(actual);

    assertTrue(expected.isEmpty());
    }

  @Test
  public void testHeader() throws IOException
    {
    copyFromLocal(testDelimited);

    Class[] types = new Class[]{String.class, String.class, String.class, String.class, long.class};
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    Tap input = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", types, testDelimited, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", types, getOutputPath( "header" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 11, 5 );
    }

  @Test
  public void testHeaderAll() throws IOException
    {
    copyFromLocal(testDelimited);

    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    Tap input = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", null, testDelimited, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( Fields.ALL, true, true, ",", "\"", null, getOutputPath( "headerall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 11, 5 );
    }

  @Test
  @Ignore //TODO: writeHeader not supported
  public void testHeaderFieldsAll() throws IOException
    {
    copyFromLocal(testDelimitedHeader);

    Tap input = getPlatform().getDelimitedFile( Fields.UNKNOWN, true, true, ",", "\"", null, testDelimitedHeader, SinkMode.KEEP );
    Tap output1 = getPlatform().getDelimitedFile( Fields.ALL, true, true, ",", "\"", null, getOutputPath( "headerfieldsall1" ), SinkMode.REPLACE );
    Tap output2 = getPlatform().getDelimitedFile( Fields.ALL, true, true, ",", "\"", null, getOutputPath( "headerfieldsall2" ), SinkMode.REPLACE );

    Tap output = new MultiSinkTap( output1, output2 );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );
    TupleEntryIterator iterator = flow.openTapForRead( getPlatform().getDelimitedFile( fields, true, true, ",", "\"", null, output1.getIdentifier(), SinkMode.REPLACE ) );

    validateLength( iterator, 13, 5 );

    assertHeaders( output1, flow );
    assertHeaders( output2, flow );
    }

  private void assertHeaders( Tap output, Flow flow ) throws IOException
    {
    TupleEntryIterator iterator = flow.openTapForRead( getPlatform().getTextFile( new Fields( "line" ), output.getIdentifier() ) );

    assertEquals( iterator.next().getObject( 0 ), "first,second,third,fourth,fifth" );

    iterator.close();
    }

  @Test
  public void testStrict() throws IOException
    {
    Fields fields = new Fields( "first", "second", "third", "fourth", "fifth" );

    copyFromLocal(testDelimitedExtraField);

    Tap input = getPlatform().getDelimitedFile( fields, false, false, ",", "\"", null, testDelimitedExtraField, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, false, false, ",", "\"", null, getOutputPath( "strict" + ComparePlatformsTest.NONDETERMINISTIC ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    try
      {
      flow.complete();
      fail( "should fail on too many fields" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testFieldCoercion() throws IOException
    {
    // 75.185.76.245 - - [01/Sep/2007:00:01:03 +0000] "POST /mt-tb.cgi/235 HTTP/1.1" 403 174 "-" "Opera/9.10 (Windows NT 5.1; U; ru)" "-"

    DateType dateType = new DateType( TestConstants.APACHE_DATE_FORMAT, TimeZone.getDefault(), Locale.US );

    Type[] types = new Type[]{
      String.class, // ip
      String.class, // -
      String.class, // -
      dateType, // date
      String.class, // request
      int.class, // code
      long.class, // bytes
      String.class, // -
      String.class, // agent
      String.class // -
    };

    Fields fields = new Fields( "ip", "client", "user", "date", "request", "code", "bytes", "referrer", "agent", "na" );

    fields = fields.applyTypes( types );

    copyFromLocal(inputFileApacheClean);

    Tap input = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", null, inputFileApacheClean, SinkMode.KEEP );
    Tap output = getPlatform().getDelimitedFile( fields, true, true, ",", "\"", null, getOutputPath( getTestName() ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipe" );

    pipe = new Each( pipe, new Fields( "date" ), AssertionLevel.STRICT, new AssertExpression( "date instanceof Long", Object.class ) );

    Flow flow = getPlatform().getFlowConnector().connect( input, output, pipe );

    flow.complete();

    validateLength( flow, 9, 10 );
    }
  }
