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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import cascading.flow.Flow;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.Retain;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.junit.Test;

import static data.InputData.*;

public class MergePipesPlatformTest extends PlatformTestCase
  {
  public MergePipesPlatformTest()
    {
    super( true ); // leave cluster testing enabled
    }

  @Test
  public void testSimpleMerge() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simplemerge" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10 );

    Collection results = getSinkAsList( flow );

    assertTrue( "missing value", results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "1\tA" ) ) );
    }

  /**
   * Specifically tests GroupBy will return the correct grouping fields to the following Every
   *
   * @throws Exception
   */
  @Test
  public void testSimpleMergeThree() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethree" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper, pipeOffset );

    splice = new Each( splice, new Fields( "num", "char" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSimpleMergeThreeChain() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethreechain" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new Merge( splice, pipeOffset );

    splice = new Each( splice, new Fields( "num", "char" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSimpleMergeThreeChainGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethreechaingroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new Merge( splice, pipeOffset );

    splice = new GroupBy( splice, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 14 );
    }

  @Test
  public void testSimpleMergeThreeChainCoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simplemergethreechaincogroup" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    splice = new CoGroup( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 6 );
    }

  @Test
  public void testSameSourceMergeThreeChainGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );

    Map sources = new HashMap();

    sources.put( "split", sourceLower );

    Tap sink = getPlatform().getTextFile( getOutputPath( "samemergethreechaingroup" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipe = new Pipe( "split" );

    Pipe pipeLower = new Each( new Pipe( "lower", pipe ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper", pipe ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset", pipe ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    //put group before merge to test path counts
    splice = new GroupBy( splice, new Fields( "num" ) );

    splice = new Merge( splice, pipeOffset );

    // this group has its incoming paths counted, gated by the previous group
    splice = new GroupBy( splice, new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 15 );
    }

  @Test
  public void testSplitSameSourceMerged() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemerged" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new Merge( "merged", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 3 );
    }

  @Test
  public void testSplitSameSourceMergedComplex() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemergedcomplex" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new Merge( "merged-first", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    left = new Each( new Pipe( "left", merged ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    right = new Each( new Pipe( "right", merged ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    merged = new Merge( "merged-second", left, right );

    merged = new Each( merged, new Fields( "line" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 3 );
    }

  @Test
  public void testSimpleMergeFail() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simplemergefail" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    pipeLower = new Rename( pipeLower, new Fields( "num" ), new Fields( "num2" ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    try
      {
      Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );
      fail();
      }
    catch( Exception exception )
      {
//      exception.printStackTrace();
      // ignore
      }
    }

  @Test
  public void testMergeIntoHashJoinStreamed() throws Exception
    {
    runMergeIntoHashJoin( true );
    }

  @Test
  public void testMergeIntoHashJoinAccumulated() throws Exception
    {
    runMergeIntoHashJoin( false );
    }

  private void runMergeIntoHashJoin( boolean streamed ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    String name = streamed ? "streamed" : "accumulated";
    String path = "mergeintohashjoin" + name;
    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    if( streamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

//    flow.writeDOT( name + ".dot" );

    // two jobs, we must put a temp tap between the Merge and HashJoin
    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 1, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 6 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamed() throws Exception
    {
    runHashJoinIntoMergeIntoHashJoin( true );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulated() throws Exception
    {
    runHashJoinIntoMergeIntoHashJoin( false );
    }

  private void runHashJoinIntoMergeIntoHashJoin( boolean streamed ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    String name = streamed ? "streamed" : "accumulated";
    String path = "hashjoinintomergeintohashjoin" + name;
    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    splice = new Merge( "merge", splice, pipeUpper );

    if( streamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

//    flow.writeDOT( name + ".dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", streamed ? 1 : 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 8 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamedStreamedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, true, true, 1 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedAccumulatedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, false, true, 3 );
    }

  /**
   * This test will exercise the issue where a unconnected HashJoin could be accumulated against within
   * a node.
   */
  @Test
  public void testHashJoinMergeIntoHashJoinStreamedAccumulatedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, false, true, 2 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedStreamedMerge() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, true, true, 3 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamedStreamed() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, true, false, 1 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedAccumulated() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, false, false, 3 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinStreamedAccumulated() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( true, false, false, 2 );
    }

  @Test
  public void testHashJoinMergeIntoHashJoinAccumulatedStreamed() throws Exception
    {
    runMultiHashJoinIntoMergeIntoHashJoin( false, true, false, 3 );
    }

  private void runMultiHashJoinIntoMergeIntoHashJoin( boolean firstStreamed, boolean secondStreamed, boolean interMerge, int expectedSteps ) throws IOException
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );
    getPlatform().copyFromLocal( inputFileLowerOffset );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );
    Tap sourceLowerOffset = getPlatform().getTextFile( inputFileLowerOffset );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );
    sources.put( "offset", sourceLowerOffset );

    String name = firstStreamed ? "firstStreamed" : "firstAccumulated";
    name += secondStreamed ? "secondStreamed" : "secondAccumulated";
    name += interMerge ? "interMerge" : "noInterMerge";

    String path = "multihashjoinintomergeintohashjoin" + name;
    Tap sink = getPlatform().getTextFile( getOutputPath( path ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), new RegexSplitter( new Fields( "num1", "char1" ), " " ) );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), new RegexSplitter( new Fields( "num2", "char2" ), " " ) );

    Pipe splice = new HashJoin( pipeLower, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    splice = new Merge( "merge1", splice, pipeUpper );

    if( firstStreamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    splice = new Retain( splice, new Fields( "num1", "char1" ) );

    if( interMerge )
      splice = new Merge( "merge2", splice, pipeUpper );

    if( secondStreamed )
      splice = new HashJoin( splice, new Fields( "num1" ), pipeOffset, new Fields( "num2" ) );
    else
      splice = new HashJoin( pipeOffset, new Fields( "num2" ), splice, new Fields( "num1" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

//    flow.writeDOT( name + ".dot" );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", expectedSteps, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, interMerge ? 17 : 14 );
    }

  @Test
  public void testGroupByAggregationMerge() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath(), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );

    pipeLower = new GroupBy( pipeLower, new Fields( "num" ) );
    pipeLower = new Every( pipeLower, new Fields( "char" ), new First( Fields.ARGS ) );

    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    pipeUpper = new GroupBy( pipeUpper, new Fields( "num" ) );
    pipeUpper = new Every( pipeUpper, new Fields( "char" ), new First( Fields.ARGS ) );

    Pipe splice = new Merge( "merge", pipeLower, pipeUpper );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10 );

    Collection results = getSinkAsList( flow );

    assertTrue( "missing value", results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "1\tA" ) ) );
    }
  }
