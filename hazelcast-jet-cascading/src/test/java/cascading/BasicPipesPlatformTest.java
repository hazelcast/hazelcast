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

package cascading;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.flow.Flow;
import cascading.operation.Aggregator;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.Count;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;

/**
 * These tests execute basic function using field positions, not names. so there will be duplicates with
 * FieldedPipesPlatformTest
 */
public class BasicPipesPlatformTest extends PlatformTestCase
  {
  public BasicPipesPlatformTest()
    {
    }

  /**
   * Test the count aggregator function
   *
   * @throws IOException
   */
  @Test
  public void testCount() throws Exception
    {
    runTestCount( "count", new Fields( 1 ), new Fields( 0 ), new Fields( 0, 1 ) );
    }

  @Test
  public void testCount2() throws Exception
    {
    runTestCount( "count2", new Fields( 1 ), new Fields( "count" ), new Fields( 0, "count" ) );
    }

  @Test
  public void testCount3() throws Exception
    {
    runTestCount( "count3", new Fields( 1 ), new Fields( "count" ), Fields.ALL );
    }

  @Test
  public void testCount4() throws Exception
    {
    runTestCount( "count4", Fields.ALL, new Fields( "count" ), Fields.ALL );
    }

  void runTestCount( String name, Fields argumentSelector, Fields fieldDeclaration, Fields outputSelector ) throws Exception
    {
    getPlatform().copyFromLocal( inputFileIps );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileIps );
    Tap sink = getPlatform().getTextFile( Fields.size( 1 ), getOutputPath( name ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "count" );
    pipe = new GroupBy( pipe, new Fields( 1 ) );
    pipe = new Every( pipe, argumentSelector, new Count( fieldDeclaration ), outputSelector );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.start(); // simple test for start
    flow.complete();

    validateLength( flow, 17 );
    assertTrue( getSinkAsList( flow ).contains( new Tuple( "63.123.238.8\t2" ) ) );
    }

  /**
   * A slightly more complex pipe
   *
   * @throws IOException
   */
  @Test
  public void testSimple() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( Fields.size( 1 ), getOutputPath( "simple" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( "^[^ ]*" );

    pipe = new Each( pipe, new Fields( 1 ), parser, new Fields( 0, 2 ) );

    // test that selector against incoming creates proper outgoing
    pipe = new Each( pipe, new Fields( 1 ), new Identity() );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Aggregator counter = new Count();

    pipe = new Every( pipe, new Fields( 0 ), counter, new Fields( 0, 1 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, 1 );
    }

  /**
   * tests that the Fields.ARGS declarator properly resolves into a declarator
   *
   * @throws Exception
   */
  @Test
  public void testSimpleResult() throws Exception
    {
    copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileLower );
    Tap sink = getPlatform().getTextFile( Fields.size( 1 ), getOutputPath( "simpleresult" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    // skip the first line
    pipe = new Each( pipe, new Fields( 0 ), new ExpressionFilter( "$0 == 0", Long.class ) );

    pipe = new Each( pipe, new Fields( 1 ), new Identity() );

    pipe = new Each( pipe, Fields.ALL, new RegexFilter( "a|b|c" ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Aggregator counter = new Count();

    pipe = new Every( pipe, new Fields( 0 ), counter, new Fields( 0, 1 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 2, 1 );
    }

  @Test
  public void testSimpleRelative() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "simplerelative" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( "^[^ ]*" );

    pipe = new Each( pipe, new Fields( -1 ), parser, new Fields( -1 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Aggregator counter = new Count();

    pipe = new Every( pipe, new Fields( 0 ), counter, new Fields( 0, 1 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8 );
    }

  @Test
  public void testSimpleRelativeUnknown() throws Exception
    {
    copyFromLocal( inputFileLower );

    Tap source = getPlatform().getDelimitedFile( Fields.UNKNOWN, " ", inputFileLower );
    Tap sink = getPlatform().getTextFile( getOutputPath( "simplerelativeunknown" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new GroupBy( pipe, new Fields( -1 ) );

    Aggregator counter = new Count();

    pipe = new Every( pipe, new Fields( 0 ), counter, new Fields( 0, 1 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );
    }

  @Test
  public void testCoGroup() throws Exception
    {
    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( Fields.size( 2 ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( Fields.size( 2 ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    // using null pos so all fields are written
    Tap sink = getPlatform().getTextFile( Fields.size( 1 ), getOutputPath( "complexcogroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( Fields.size( 2 ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( 1 ), splitter, Fields.RESULTS );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( 1 ), splitter, Fields.RESULTS );

    Pipe splice = new CoGroup( pipeLower, new Fields( 0 ), pipeUpper, new Fields( 0 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( results.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testCoGroupRelativeUnknown() throws Exception
    {
    copyFromLocal( inputFileLower );
    copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getDelimitedFile( Fields.UNKNOWN, " ", inputFileLower );
    Tap sourceUpper = getPlatform().getDelimitedFile( Fields.UNKNOWN, " ", inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    // using null pos so all fields are written
    Tap sink = getPlatform().getTextFile( Fields.size( 1 ), getOutputPath( "complexcogrouprelativeunknown" ), SinkMode.REPLACE );

    Pipe pipeLower = new Pipe( "lower" );
    Pipe pipeUpper = new Pipe( "upper" );

    Pipe splice = new CoGroup( pipeLower, new Fields( -2 ), pipeUpper, new Fields( -2 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( results.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testUnGroup() throws Exception
    {
    copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "ungrouped" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( 1 ), new RegexSplitter( Fields.size( 3 ) ) );

    pipe = new Each( pipe, new UnGroup( Fields.size( 2 ), new Fields( 0 ), Fields.fields( new Fields( 1 ), new Fields( 2 ) ) ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10 );
    }

  @Test
  public void testFilterAll() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "filterall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( ".*", true );

    pipe = new Each( pipe, new Fields( 1 ), filter );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 0 );
    }

  @Test
  public void testFilter() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "filter" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( "^68.*" );

    pipe = new Each( pipe, new Fields( 1 ), filter );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 3 );
    }

  @Test
  public void testSimpleChain() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "simplechain" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( "^[^ ]*" );

    pipe = new Each( pipe, new Fields( 1 ), parser, new Fields( 2 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    pipe = new Every( pipe, new Fields( 0 ), new Count(), new Fields( 0, 1 ) );

    // add a second group to force a new map/red
    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8 );
    }

  @Test
  public void testReplace() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "replace" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( Fields.ARGS, "^[^ ]*" );
    pipe = new Each( pipe, new Fields( 1 ), parser, Fields.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, 2, Pattern.compile( "\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}" ) );
    }

  @Test
  public void testSwap() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( Fields.size( 2 ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "swap" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( 0 ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( 1 ), parser, Fields.SWAP );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, 2, Pattern.compile( "^\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}$" ) );
    }
  }
