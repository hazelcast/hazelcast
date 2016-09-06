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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.Insert;
import cascading.operation.aggregator.Count;
import cascading.operation.buffer.FirstNBuffer;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.BufferJoin;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;

public class BufferPipesPlatformTest extends PlatformTestCase
  {
  public BufferPipesPlatformTest()
    {
    super( false ); // no need for clustering
    }

  @Test
  public void testSimpleBuffer() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simple" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new TestBuffer( new Fields( "next" ), 2, true, true, "next" ) );

    pipe = new Each( pipe, new Insert( new Fields( "final" ), "final" ), Fields.ALL );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 23, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\tnull\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\ta\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\tb\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\tc\tnext\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "1\tnull\tnext\tfinal" ) ) );
    }

  @Test
  public void testSimpleBuffer2() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simple2" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new Fields( "lower" ), new TestBuffer( new Fields( "next" ), 1, true, "next" ), Fields.RESULTS );

    pipe = new Each( pipe, new Insert( new Fields( "final" ), "final" ), Fields.ALL );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 18, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "next\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "next\tfinal" ) ) );
    assertTrue( results.contains( new Tuple( "next\tfinal" ) ) );
    }

  @Test
  public void testSimpleBuffer3() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simple3" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, new TestBuffer( new Fields( "new" ), new Tuple( "new" ) ), new Fields( "new", "lower", "upper" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 5, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "new\ta\tA" ) ) );
    assertTrue( results.contains( new Tuple( "new\tb\tB" ) ) );
    assertTrue( results.contains( new Tuple( "new\tc\tC" ) ) );
    }

  /**
   * tests wildcard fields are properly resolving.
   *
   * @throws Exception
   */
  @Test
  public void testIdentityBuffer() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getTextFile( inputFileLhs );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "identity" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower" ), "\\s" ) );

    pipe = new GroupBy( pipe, new Fields( "num" ) );

    pipe = new Every( pipe, Fields.VALUES, new TestBuffer( Fields.ARGS ), Fields.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 13 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( results.contains( new Tuple( "1\tb" ) ) );
    assertTrue( results.contains( new Tuple( "1\tc" ) ) );
    }

  @Test
  public void testJoinerClosure() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "cogroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new BufferJoin() );

    splice = new Every( splice, new InnerJoinTestBuffer( Fields.size( 4 ) ), Fields.RESULTS );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 5 );

    List<Tuple> values = getSinkAsList( flow );

    assertTrue( values.contains( new Tuple( "1\ta\t1\tA" ) ) );
    assertTrue( values.contains( new Tuple( "2\tb\t2\tB" ) ) );
    }

  @Test
  public void testJoinerClosureFail() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), "failpath", SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new CoGroup( pipeLower, new Fields( "num" ), pipeUpper, new Fields( "num" ), new BufferJoin() );

    splice = new Every( splice, Fields.size( 1 ), new Count(), Fields.RESULTS );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
      fail();
      }
    catch( Exception exception )
      {
      // success
      assertTrue( exception.getMessage().contains( "Fields.NONE" ) );
//      exception.printStackTrace();
      }
    }

  @Test
  public void testFirstNBuffer() throws Exception
    {
    Set<String> expected = new HashSet<String>();

    expected.add( "1" );
    expected.add( "2" );
    expected.add( "3" );
    expected.add( "4" );
    expected.add( "5" );

    Flow flow = runFirstNBuffer( false, false );

    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    for (Tuple result : results)
      {
        expected.remove(result.getString(0));
      }

      assertTrue( expected.isEmpty() );
    }

  @Test
  public void testFirstNBufferForward() throws Exception
    {
    Set<Tuple> expected = new HashSet<>();

    expected.add( new Tuple( "1", "a" ) );
    expected.add( new Tuple( "2", "b" ) );
    expected.add( new Tuple( "3", "c" ) );
    expected.add( new Tuple( "4", "b" ) );
    expected.add( new Tuple( "5", "a" ) );

    Flow flow = runFirstNBuffer( true, false );
    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    expected.removeAll( results );
    assertTrue( expected.isEmpty() );
    }

  @Test
  public void testFirstNBufferReverse() throws Exception
    {
    Set<Tuple> expected = new HashSet<>();

    expected.add( new Tuple( "1", "c" ) );
    expected.add( new Tuple( "2", "d" ) );
    expected.add( new Tuple( "3", "c" ) );
    expected.add( new Tuple( "4", "d" ) );
    expected.add( new Tuple( "5", "e" ) );

    Flow flow = runFirstNBuffer( true, true );
    validateLength( flow, 5 );

    List<Tuple> results = getSinkAsList( flow );

    expected.removeAll( results );
    assertTrue( expected.isEmpty() );
    }

  /**
   * this family of tests verify an iterator can be abandoned mid iteration without affecting posterior aggregations
   */
  protected Flow runFirstNBuffer( boolean secondarySort, boolean reverseOrder ) throws Exception
    {
    getPlatform().copyFromLocal( inputFileLhs );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "lower" ), " ", inputFileLhs );
    Tap sink = getPlatform().getDelimitedFile( new Fields( "num", "lower" ), "\t", getOutputPath( "firstn" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    if( !secondarySort )
      pipe = new GroupBy( pipe, new Fields( "num" ) );
    else
      pipe = new GroupBy( pipe, new Fields( "num" ), new Fields( "lower" ), reverseOrder );

    pipe = new Every( pipe, Fields.VALUES, new FirstNBuffer( Fields.ARGS, 1 ), Fields.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    return flow;
    }
  }