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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.flow.FlowConnectorProps;
import cascading.operation.AssertionLevel;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.aggregator.Sum;
import cascading.operation.assertion.AssertSizeMoreThan;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.And;
import cascading.operation.filter.Not;
import cascading.operation.filter.Or;
import cascading.operation.filter.Xor;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
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

public class RegressionPipesPlatformTest extends PlatformTestCase
  {
  public RegressionPipesPlatformTest()
    {
    super( false );
    }

  /**
   * tests that a selector will select something other than the first position from an UNKNOWN tuple
   *
   * @throws Exception
   */
  @Test
  public void testUnknown() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "unknown" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( Fields.UNKNOWN ) );

    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( 2 ), new Identity( new Fields( "label" ) ) );

    pipe = new Each( pipe, new Debug() );

    pipe = new Each( pipe, new Fields( "label" ), new RegexFilter( "[A-Z]*" ) );

    pipe = new Each( pipe, new Debug() );

    Map<Object, Object> properties = getPlatform().getProperties();

    FlowConnectorProps.setDebugLevel( properties, DebugLevel.NONE );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );
    }

  @Test
  public void testCopy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "copy" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );
    }

  /**
   * tests that a selector will select something other than the first position from an UNKNOWN tuple
   *
   * @throws Exception
   */
  @Test
  public void testVarWidth() throws Exception
    {
    getPlatform().copyFromLocal( inputFileCritics );

    Tap source = getPlatform().getTextFile( inputFileCritics );
    Tap sink = getPlatform().getTextFile( getOutputPath( "varwidth" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( Fields.UNKNOWN ) );

    pipe = new Each( pipe, AssertionLevel.STRICT, new AssertSizeMoreThan( 3 ) );

    pipe = new Each( pipe, new Fields( 0, 1, -1 ), new Identity( new Fields( "name", "second", "last" ) ) );

    pipe = new Each( pipe, new Debug() );

    Map<Object, Object> properties = getPlatform().getProperties();

    FlowConnectorProps.setDebugLevel( properties, DebugLevel.NONE );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 7 );
    }

  /**
   * This test allows for Fields.UNKNOWN to propagate from the RegexSplitter through to the UnGroup (or any other
   * operation).
   * <p/>
   * This could be dangerous but feels very natural and part of the intentions of having UNKNOWN
   *
   * @throws Exception
   */
  @Test
  public void testUnGroupUnknown() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "ungrouped-unknown-nondeterministic" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    // emits Fields.UNKNOWN
    pipe = new Each( pipe, new Fields( 1 ), new RegexSplitter( "\t" ), Fields.ALL );

    // accepts Fields.UNKNOWN
    pipe = new Each( pipe, new UnGroup( Fields.size( 2 ), new Fields( 0 ), Fields.fields( new Fields( 1 ), new Fields( 2 ) ) ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10 );
    }

  @Test
  public void testDupeHeadNames() throws Exception
    {
    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "unknown" ), SinkMode.REPLACE );

    Pipe lhs = new Pipe( "test" );

    lhs = new Each( lhs, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe rhs = new Pipe( "test" );

    rhs = new Each( rhs, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe group = new GroupBy( Pipe.pipes( lhs, rhs ), Fields.size( 3 ) );

    try
      {
      getPlatform().getFlowConnector().connect( source, sink, group );
      fail( "did not fail on dupe head names" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testDupeTailNames() throws Exception
    {
    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "unknown" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe group = new GroupBy( pipe, Fields.size( 3 ) );

    Pipe lhs = new Pipe( "tail", group );
    lhs = new Each( group, new Fields( "line" ), new RegexSplitter( " " ) );

    Pipe rhs = new Pipe( "tail", group );
    rhs = new Each( group, new Fields( "line" ), new RegexSplitter( " " ) );

    Map<String, Tap> sinks = Cascades.tapsMap( Pipe.pipes( lhs, rhs ), Tap.taps( sink, sink ) );

    try
      {
      getPlatform().getFlowConnector().connect( source, sinks, Pipe.pipes( lhs, rhs ) );
      fail( "did not fail on dupe head names" );
      }
    catch( Exception exception )
      {
      // ignore
      }
    }

  @Test
  public void testIllegalCharsInTempFiles() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "illegalchars" ), SinkMode.REPLACE );

//    Pipe pipe = new Pipe( "bar:bar@foo://blah/\t(*(**^**&%&%^@#@&&() :::: ///\\\\ \t illegal chars in it" );
    Pipe pipe = new Pipe( "**&%&%bar:bar@foo://blah/\t(*(**^**&%&%^@#@&&() :::: ///\\\\ \t illegal chars in it" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( " " ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    pipe = new GroupBy( pipe, new Fields( 0 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );
    }

  /**
   * Method testCoGroupSplitPipe tests the case where CoGroup on the lhs steps on the tuple as it passes down
   * the rhs. this is rare and expects that one side is all filters.
   *
   * @throws Exception when
   */
  @Test
  public void testCoGroupSplitPipe() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileLower );
    Tap splitTap = getPlatform().getTabDelimitedFile( new Fields( "num", "char" ), getOutputPath( "intermediate" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe split = new Each( "split", splitter );

    Flow splitFlow = getPlatform().getFlowConnector().connect( source, splitTap, split );

    splitFlow.complete();

    // using null pos so all fields are written
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroupsplit" ), SinkMode.REPLACE );

    Pipe lower = new Pipe( "lower" );

    Pipe lhs = new Pipe( "lhs", lower );

//    lhs = new Each( lhs, new Identity() ); // identity does not trigger the issue this tests.
//    lhs = new Each( lhs, new Debug( "lhs", true ) );

    Pipe rhs = new Pipe( "rhs", lower );

    rhs = new Each( rhs, new Debug( "rhs-pre", true ) );

    rhs = new Each( rhs, new Fields( "num" ), new Identity( new Fields( "num2" ) ) );

    rhs = new Each( rhs, new Debug( "rhs-post", true ) );

    Pipe cogroup = new CoGroup( lhs, new Fields( "num" ), rhs, new Fields( "num2" ) );

//    cogroup = new Each( cogroup, new Debug( true ) );

    Flow flow = getPlatform().getFlowConnector().connect( splitTap, sink, cogroup );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\ta\t1" ) ) );
    assertTrue( results.contains( new Tuple( "2\tb\t2" ) ) );
    }

  /**
   * Method testCoGroupSplitPipe tests the case where GroupBy on the lhs steps on the tuple as it passes down
   * the rhs. this is rare and expects that one side is all filters.
   *
   * @throws Exception when
   */
  @Test
  public void testGroupBySplitPipe() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileLower );
    Tap splitTap = getPlatform().getTabDelimitedFile( new Fields( "num", "char" ), getOutputPath( "splitintermediate" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe split = new Each( "split", splitter );

    Flow splitFlow = getPlatform().getFlowConnector().connect( source, splitTap, split );

    splitFlow.complete();

    // using null pos so all fields are written
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "groupbysplit" ), SinkMode.REPLACE );

    Pipe lower = new Pipe( "lower" );

    Pipe lhs = new Pipe( "lhs", lower );

    Pipe rhs = new Pipe( "rhs", lower );

    rhs = new Each( rhs, new Fields( "num" ), new Identity( new Fields( "num2" ) ), new Fields( "num", "char" ) );

    Pipe groupBy = new GroupBy( Pipe.pipes( lhs, rhs ), new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( splitTap, sink, groupBy );

    flow.complete();

    validateLength( flow, 10, null );

    List<Tuple> results = getSinkAsList( flow );

    assertEquals( 2, Collections.frequency( results, new Tuple( "1\ta" ) ) );
    assertEquals( 2, Collections.frequency( results, new Tuple( "2\tb" ) ) );
    }

  @Test
  public void testLastEachNotModified() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new TestFunction( new Fields( "insert" ), new Tuple( "inserted" ) ) );

    pipe = new GroupBy( pipe, new Fields( "insert" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "lasteachmodified" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );
    }

  @Test
  public void testComplexLogicAnd() throws Exception
    {
    copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new Not( new And( new Fields( "num" ), new RegexFilter( "1", true, true ), new Fields( "char" ), new RegexFilter( "a", true, true ) ) );

    // compounding the filter for the Fields.ALL case.
    pipe = new Each( pipe, filter );
    pipe = new Each( pipe, new Fields( "num", "char" ), filter );

    Tap sink = getPlatform().getDelimitedFile( Fields.ALL, " ", getOutputPath( "/regression/complexlogicand" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 1, 2, Pattern.compile( "1\ta" ) );
    }

  @Test
  public void testComplexLogicOr() throws Exception
    {
    copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new Not( new Or( new Fields( "num" ), new RegexFilter( "1", true, true ), new Fields( "char" ), new RegexFilter( "a", true, true ) ) );

    // compounding the filter for the Fields.ALL case.
    pipe = new Each( pipe, filter );
    pipe = new Each( pipe, new Fields( "num", "char" ), filter );

    Tap sink = getPlatform().getDelimitedFile( Fields.ALL, " ", getOutputPath( "/regression/complexlogicor" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 4, 2, Pattern.compile( "(1\t.)|(.\ta)" ) );
    }

  @Test
  public void testComplexLogicXor() throws Exception
    {
    copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLhs );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new Not( new Xor( new Fields( "num" ), new RegexFilter( "1", true, true ), new Fields( "char" ), new RegexFilter( "a", true, true ) ) );

    // compounding the filter for the Fields.ALL case.
    pipe = new Each( pipe, filter );
    pipe = new Each( pipe, new Fields( "num", "char" ), filter );

    Tap sink = getPlatform().getDelimitedFile( Fields.ALL, " ", getOutputPath( "/regression/complexlogicxor" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 3, 2, Pattern.compile( "(1\t.)|(.\ta)" ) );
    }

  @Test
  public void testGroupNoneSortUnknown() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "none-unknown" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( 1 ), new RegexSplitter( "\t" ) );

    pipe = new GroupBy( pipe, Fields.NONE, Fields.FIRST );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5 );
    }

  @Test
  public void testDeepPipeline() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache200 );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileApache200 );
    Tap sink = getPlatform().getTextFile( getOutputPath( "deeppipline" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "pipeline" );

    Function function = new ExpressionFunction( new Fields( "count" ), "line.split( \"\\\\s\").length", String.class );
    pipe = new Each( pipe, new Fields( "line" ), function, Fields.ALL );

    int depth = 50;

    for( int i = 0; i < depth; i++ )
      {
      pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "count" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "line" ), new Identity(), Fields.REPLACE );
      pipe = new Each( pipe, new Fields( "count" ), new Identity(), Fields.REPLACE );
      pipe = new Each( pipe, new Fields( "line", "count" ), new Identity() );
      pipe = new Each( pipe, new Fields( "line", "count" ), new Identity( new Fields( "line2", "count2" ) ), new Fields( "line", "count2" ) );
      pipe = new Each( pipe, new Fields( "count2" ), new Identity( new Fields( "count" ) ), new Fields( "line", "count" ) );
      }

    int modulo = 1000000;

    pipe = new Each( pipe, new Fields( "line" ), new ExpressionFunction( new Fields( "hash" ), "line.hashCode() % " + modulo, String.class ), Fields.ALL ); // want some collisions

    pipe = new GroupBy( pipe, new Fields( "hash" ) );

    for( int i = 0; i < depth; i++ )
      pipe = new Every( pipe, new Fields( "count" ), new Sum( new Fields( "sum" + ( i + 1 ) ) ) );

    for( int i = 0; i < depth; i++ )
      {
      pipe = new Each( pipe, new Fields( "hash" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "sum1" ), new Identity( new Fields( 0 ) ), Fields.ALL );
      pipe = new Each( pipe, new Fields( "hash", "sum1" ), new Identity(), Fields.SWAP );
      }

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 200 );
    }

  @Test
  public void testUnknownReplace() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getDelimitedFile( Fields.UNKNOWN, "-", inputFileJoined );
    Tap sink = getPlatform().getTabDelimitedFile( Fields.UNKNOWN, getOutputPath( "unknown-replace" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( 0 ), new Insert( Fields.ARGS, "value" ), Fields.REPLACE );

    pipe = new Each( pipe, new Debug() );

    Map<Object, Object> properties = getPlatform().getProperties();

    FlowConnectorProps.setDebugLevel( properties, DebugLevel.NONE );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    assertEquals( 5, asList( flow, sink ).size() );

    Set<Tuple> results = asSet( flow, sink );

    assertEquals( 1, results.size() );
    assertEquals( new Tuple( "value" ), results.iterator().next() );
    }

  @Test
  public void testOOMEPreGroup() throws Exception
    {
    copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new TestFunction( new Fields( "insert" ), null )
    {
    @Override
    protected void throwIntentionalException()
      {
      throw new OutOfMemoryError( "fake error" );
      }
    } );

    Tap sink = getPlatform().getTextFile( getOutputPath( "oomepre" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    try
      {
      flow.complete();
      fail( "no failure thrown" );
      }
    catch( Throwable exception )
      {
      System.out.println( "exception = " + exception );
      exception.printStackTrace();

      // test local mode passes the OOME on, other its squashed, but the flow should fail
      if( !getPlatform().isMapReduce() && !getPlatform().isDAG() )
        assertTrue( exception instanceof OutOfMemoryError );
      }
    }

  @Test
  public void testOOMEPostGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "oomepost" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), Fields.size( 4 ) );

    splice = new Each( splice, Fields.NONE, new TestFunction( new Fields( "insert" ), null )
    {
    @Override
    protected void throwIntentionalException()
      {
      throw new OutOfMemoryError( "fake error" );
      }
    } );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    try
      {
      flow.complete();
      fail( "no failure thrown" );
      }
    catch( Throwable exception )
      {
      System.out.println( "exception = " + exception );
      exception.printStackTrace();

      // test local mode passes the OOME on, other its squashed, but the flow should fail
      if( !getPlatform().isMapReduce() && !getPlatform().isDAG() )
        assertTrue( exception instanceof OutOfMemoryError );
      }
    }
  }