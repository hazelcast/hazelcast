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

package cascading.operation.assertion;

import java.io.IOException;
import java.util.Map;

import cascading.PlatformTestCase;
import cascading.TestConstants;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.operation.AssertionLevel;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.junit.Test;

import static data.InputData.inputFileApache;

/**
 *
 */
public class AppliedAssertionsPlatformTest extends PlatformTestCase
  {
  private String apacheCommonRegex = TestConstants.APACHE_COMMON_REGEX;
  private RegexParser apacheCommonParser = new RegexParser( new Fields( "ip", "time", "method", "event", "status", "size" ), apacheCommonRegex,
    new int[]{1, 2, 3, 4, 5, 6} );

  public AppliedAssertionsPlatformTest()
    {
    }

  @Test
  public void testValueAssertionsPass() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "value/pass" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, new Count(), new Fields( "method", "count" ) ); // count is a long value

    pipe = new Each( pipe, new Fields( "count" ), AssertionLevel.STRICT, new AssertEquals( 7L ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 1, null );
    }

  @Test
  public void testValueAssertionsFail() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "value/fail" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, new Count(), new Fields( "method", "count" ) ); // count is a long value

    pipe = new Each( pipe, new Fields( "count" ), AssertionLevel.STRICT, new AssertEquals( 0L ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail( "no assertions thrown" );
      }
    catch( Exception exception )
      {

      }
    }

  @Test
  public void testValueAssertionsRemoval() throws Exception
    {
    runValueAssertions( AssertionLevel.NONE, AssertionLevel.STRICT, true );
    runValueAssertions( AssertionLevel.VALID, AssertionLevel.STRICT, true );
    runValueAssertions( AssertionLevel.STRICT, AssertionLevel.STRICT, false );

    runValueAssertions( AssertionLevel.NONE, AssertionLevel.VALID, true );
    runValueAssertions( AssertionLevel.VALID, AssertionLevel.VALID, false );
    }

  private void runValueAssertions( AssertionLevel planLevel, AssertionLevel setLevel, boolean pass ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "value/" + planLevel + "/" + setLevel ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, setLevel, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), setLevel, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, new Count(), new Fields( "method", "count" ) ); // count is a long value

    pipe = new Each( pipe, new Fields( "count" ), setLevel, new AssertEquals( 0L ) );

    Map<Object, Object> properties = getPlatform().getProperties();

    FlowConnectorProps.setAssertionLevel( properties, planLevel );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    try
      {
      flow.complete();

      if( !pass )
        fail( String.format( "no assertions thrown %s %s %s", planLevel, setLevel, pass ) );
      }
    catch( Exception exception )
      {
      if( pass )
        fail( String.format( "assertion thrown %s %s %s", planLevel, setLevel, pass ) );
      }

    if( pass )
      validateLength( flow, 1, null );
    }

  @Test
  public void testGroupAssertionsPass() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "pass" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, new Count(), new Fields( "method", "count" ) ); // count is a long value

    pipe = new Every( pipe, AssertionLevel.STRICT, new AssertGroupSizeEquals( 7L ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 1, null );
    }

  @Test
  public void testGroupAssertionsFail() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "fail" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), AssertionLevel.STRICT, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, new Count(), new Fields( "method", "count" ) ); // count is a long value

    pipe = new Every( pipe, AssertionLevel.STRICT, new AssertGroupSizeEquals( 0L ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail( "no assertions thrown" );
      }
    catch( Exception exception )
      {

      }
    }

  @Test
  public void testGroupAssertionsRemoval() throws Exception
    {
    runGroupAssertions( AssertionLevel.NONE, AssertionLevel.STRICT, true );
    runGroupAssertions( AssertionLevel.VALID, AssertionLevel.STRICT, true );
    runGroupAssertions( AssertionLevel.STRICT, AssertionLevel.STRICT, false );

    runGroupAssertions( AssertionLevel.NONE, AssertionLevel.VALID, true );
    runGroupAssertions( AssertionLevel.VALID, AssertionLevel.VALID, false );
    }

  private void runGroupAssertions( AssertionLevel planLevel, AssertionLevel setLevel, boolean pass ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "group/" + planLevel + "/" + setLevel ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), apacheCommonParser );

    pipe = new Each( pipe, setLevel, new AssertNotNull() );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), setLevel, new AssertMatches( "^POST" ) );

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Every( pipe, new Count(), new Fields( "method", "count" ) ); // count is a long value

    pipe = new Every( pipe, setLevel, new AssertGroupSizeEquals( 0L ) );

    Map<Object, Object> properties = getPlatform().getProperties();

    FlowConnectorProps.setAssertionLevel( properties, planLevel );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    try
      {
      flow.complete();

      if( !pass )
        fail( String.format( "no assertions thrown %s %s %s", planLevel, setLevel, pass ) );
      }
    catch( Exception exception )
      {
      if( pass )
        fail( String.format( "assertion thrown %s %s %s", planLevel, setLevel, pass ) );
      }

    if( pass )
      validateLength( flow, 1, null );
    }
  }
