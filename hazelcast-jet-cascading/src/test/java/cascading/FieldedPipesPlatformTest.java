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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import cascading.cascade.Cascades;
import cascading.flow.Flow;
import cascading.operation.Debug;
import cascading.operation.Filter;
import cascading.operation.Function;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.NoOp;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.First;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.And;
import cascading.operation.function.UnGroup;
import cascading.operation.regex.RegexFilter;
import cascading.operation.regex.RegexParser;
import cascading.operation.regex.RegexSplitter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.tap.MultiSourceTap;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Hasher;
import cascading.tuple.Tuple;
import org.junit.Ignore;
import org.junit.Test;

import static cascading.ComparePlatformsTest.NONDETERMINISTIC;
import static data.InputData.*;

public class FieldedPipesPlatformTest extends PlatformTestCase
  {
  public FieldedPipesPlatformTest()
    {
    super( true, 5, 3 ); // leave cluster testing enabled
    }

  @Test
  public void testSimpleGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "simple" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow.openSource(), 10 ); // validate source, this once, as a sanity check
    validateLength( flow, 8, null );
    }

  @Test
  public void testSimpleChain() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count( new Fields( "count1" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count3" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count4" ) ) );

    Tap sink = getPlatform().getTabDelimitedFile( Fields.ALL, getOutputPath( "simplechain" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, 5 );
    }

  @Test
  public void testChainEndingWithEach() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count( new Fields( "count1" ) ) );
    pipe = new Every( pipe, new Count( new Fields( "count2" ) ) );

    pipe = new Each( pipe, new Fields( "count1", "count2" ), new ExpressionFunction( new Fields( "sum" ), "count1 + count2", int.class ), Fields.ALL );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "chaineach" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );
    }

  // also tests the RegexSplitter

  @Test
  public void testNoGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new RegexSplitter( "\\s+" ), new Fields( 1 ) );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "nogroup" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "75.185.76.245" ) ) );
    }

  @Test
  public void testCopy() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    Tap sink = getPlatform().getTextFile( getOutputPath( "copy" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, null );
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

    // using null pos so all fields are written
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "simplemerge" ), SinkMode.REPLACE );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( "merge", Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ), null, false );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 10 );

    Collection results = getSinkAsList( flow );

    assertTrue( "missing value", results.contains( new Tuple( "1\ta" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "1\tA" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "2\tb" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "2\tB" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "3\tc" ) ) );
    assertTrue( "missing value", results.contains( new Tuple( "3\tC" ) ) );
    }

  /**
   * Specifically tests GroupBy will return the correct grouping fields to the following Every
   * <p/>
   * additionally tests secondary sorting during merging
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

    Tap sink = getPlatform().getDelimitedFile( Fields.ALL, "\t", getOutputPath( "simplemergethree" ), SinkMode.REPLACE );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    Pipe pipeLower = new Each( new Pipe( "lower" ), new Fields( "line" ), splitter );
    Pipe pipeUpper = new Each( new Pipe( "upper" ), new Fields( "line" ), splitter );
    Pipe pipeOffset = new Each( new Pipe( "offset" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( "merge", Pipe.pipes( pipeLower, pipeUpper, pipeOffset ), new Fields( "num" ), new Fields( "char" ) );

    splice = new Every( splice, new Fields( "char" ), new First( new Fields( "first" ) ) );

    splice = new Each( splice, new Fields( "num", "first" ), new Identity() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    validateLength( flow, 6 );

    List<Tuple> tuples = getSinkAsList( flow );

    assertTrue( tuples.contains( new Tuple( "1", "A" ) ) );
    assertTrue( tuples.contains( new Tuple( "2", "B" ) ) );
    assertTrue( tuples.contains( new Tuple( "3", "C" ) ) );
    assertTrue( tuples.contains( new Tuple( "4", "D" ) ) );
    assertTrue( tuples.contains( new Tuple( "5", "E" ) ) );
    assertTrue( tuples.contains( new Tuple( "6", "c" ) ) );
    }

  /**
   * same test as MergePipesTest, but to test that chained groupby don't exhibit similar failures
   *
   * @throws Exception
   */
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

    //put group before merge to test path counts
    Pipe splice = new GroupBy( Pipe.pipes( pipeLower, pipeUpper ), new Fields( "num" ) );

    // this group has its incoming paths counted, gated by the previous group
    splice = new GroupBy( Pipe.pipes( splice, pipeOffset ), new Fields( "num" ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    if( getPlatform().isMapReduce() )
      assertEquals( "wrong num jobs", 2, flow.getFlowSteps().size() );

    flow.complete();

    validateLength( flow, 15 );
    }

  @Test
  public void testUnGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "ungrouped" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ) ) );

    pipe = new Each( pipe, new UnGroup( new Fields( "num", "char" ), new Fields( "num" ), Fields.fields( new Fields( "lower" ), new Fields( "upper" ) ) ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10 );
    }

  @Test
  public void testUnGroupAnon() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoined );

    Tap source = getPlatform().getTextFile( inputFileJoined );
    Tap sink = getPlatform().getTextFile( getOutputPath( "ungroupedanon" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num", "lower", "upper" ) ) );

    pipe = new Each( pipe, new UnGroup( new Fields( "num" ), Fields.fields( new Fields( "lower" ), new Fields( "upper" ) ) ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10 );
    }

  @Test
  public void testUnGroupBySize() throws Exception
    {
    getPlatform().copyFromLocal( inputFileJoinedExtra );

    Tap source = getPlatform().getTextFile( inputFileJoinedExtra );
    Tap sink = getPlatform().getTextFile( getOutputPath( "ungrouped_size" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( new Fields( "num1", "num2", "lower", "upper" ) ) );

    pipe = new Each( pipe, new UnGroup( new Fields( "num1", "num2", "char" ), new Fields( "num1", "num2" ), 1 ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    List<Tuple> tuples = asList( flow, sink );
    assertEquals( 10, tuples.size() );

    List<Object> values = new ArrayList<Object>();
    for( Tuple tuple : tuples )
      values.add( tuple.getObject( 1 ) );

    assertTrue( values.contains( "1\t1\ta" ) );
    assertTrue( values.contains( "1\t1\tA" ) );
    assertTrue( values.contains( "2\t2\tb" ) );
    assertTrue( values.contains( "2\t2\tB" ) );
    assertTrue( values.contains( "3\t3\tc" ) );
    assertTrue( values.contains( "3\t3\tC" ) );
    assertTrue( values.contains( "4\t4\td" ) );
    assertTrue( values.contains( "4\t4\tD" ) );
    assertTrue( values.contains( "5\t5\te" ) );
    assertTrue( values.contains( "5\t5\tE" ) );
    }

  @Test
  public void testFilter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "filter" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new RegexFilter( "^68.*" );

    pipe = new Each( pipe, new Fields( "line" ), filter );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 3 );
    }

  @Test
  public void testLogicFilter() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "logicfilter" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Filter filter = new And( new RegexFilter( "^68.*$" ), new RegexFilter( "^1000.*$" ) );

    pipe = new Each( pipe, new Fields( "line" ), filter );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 3 );
    }

  @Test
  public void testFilterComplex() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "filtercomplex" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), TestConstants.APACHE_COMMON_PARSER );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );
    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^POST" ) );

    pipe = new Each( pipe, new Fields( "method" ), new Identity( new Fields( "value" ) ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "value" ) );

    pipe = new Every( pipe, new Count(), new Fields( "value", "count" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 1, null );
    }

  /**
   * Intentionally filters all values out to test next mr job behaves
   *
   * @throws Exception
   */
  @Test
  public void testFilterAll() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "filterall" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    String regex = "^([^ ]*) +[^ ]* +[^ ]* +\\[([^]]*)\\] +\\\"([^ ]*) ([^ ]*) [^ ]*\\\" ([^ ]*) ([^ ]*).*$";
    Fields fieldDeclaration = new Fields( "ip", "time", "method", "event", "status", "size" );
    int[] groups = {1, 2, 3, 4, 5, 6};
    RegexParser function = new RegexParser( fieldDeclaration, regex, groups );
    pipe = new Each( pipe, new Fields( "line" ), function );

    pipe = new Each( pipe, new Fields( "method" ), new RegexFilter( "^fobar" ) ); // intentionally filtering all

    pipe = new GroupBy( pipe, new Fields( "method" ) );

    pipe = new Each( pipe, new Fields( "method" ), new Identity( new Fields( "value" ) ), Fields.ALL );

    pipe = new GroupBy( pipe, new Fields( "value" ) );

    pipe = new Every( pipe, new Count(), new Fields( "value", "count" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 0, null );
    }

//  public void testLimitFilter() throws Exception
//    {
//    copyFromLocal( inputFileApache );
//
//    Tap source = new Hfs( new TextLine( new Fields( "offset", "line" ) ), inputFileApache );
//    Tap sink = new Lfs( new TextLine(), outputPath + "/limitfilter", true );
//
//    Pipe pipe = new Pipe( "test" );
//
//    Filter filter = new Limit( 7 );
//
//    pipe = new Each( pipe, new Fields( "line" ), filter );
//
//    Flow flow = new FlowConnector( getProperties() ).connect( source, sink, pipe );
//
////    flow.writeDOT( "flow.dot" );
//
//    flow.complete();
//
//    validateLength( flow, 7, null );
//    }

  //

  /*
   *
   * TODO: create (optional) Tez rule to consolidate into a single DAG. currently renders to two DAGs, one for each side
   *
   */
  @Test
  public void testSplit() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( inputFileApache );
    Tap sink1 = getPlatform().getTextFile( getOutputPath( "split1" ), SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( getOutputPath( "split2" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sinks, left, right );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 2, "right" );
    }

  /**
   * verifies non-safe rules apply in the proper place
   *
   * @throws Exception
   */
  @Test
  public void testSplitNonSafe() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink1 = getPlatform().getTextFile( getOutputPath( "nonsafesplit1" ), SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( getOutputPath( "nonsafesplit2" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    // run job on non-safe operation, forces 3 mr jobs.
    pipe = new Each( pipe, new TestFunction( new Fields( "ignore" ), new Tuple( 1 ), false ), new Fields( "line" ) );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Map sources = new HashMap();
    sources.put( "split", source );

    Map sinks = new HashMap();
    sinks.put( "left", sink1 );
    sinks.put( "right", sink2 );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sinks, left, right );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 2, "right" );
    }

  @Test
  public void testSplitSameSourceMerged() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemerged" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new GroupBy( "merged", Pipe.pipes( left, right ), new Fields( "line" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    flow.complete();

    validateLength( flow, 3 );
    }

  /**
   * verifies not inserting Identity between groups works
   *
   * @throws Exception
   */
  @Test
  public void testSplitOut() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap sourceLower = getPlatform().getTextFile( new Fields( "num", "line" ), inputFileApache );

    Map sources = new HashMap();

    sources.put( "lower1", sourceLower );

    // using null pos so all fields are written
    Tap sink1 = getPlatform().getTextFile( getOutputPath( "splitout1" ), SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( getOutputPath( "splitout2" ), SinkMode.REPLACE );

    Map sinks = new HashMap();

    sinks.put( "output1", sink1 );
    sinks.put( "output2", sink2 );

    Pipe pipeLower1 = new Pipe( "lower1" );

    Pipe left = new GroupBy( "output1", pipeLower1, new Fields( 0 ) );
    Pipe right = new GroupBy( "output2", left, new Fields( 0 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sinks, Pipe.pipes( left, right ) );

//    flow.writeDOT( "spit.dot" );

    flow.complete();

    validateLength( flow, 10, "output1" );
    validateLength( flow, 10, "output2" );

    assertEquals( 10, asSet( flow, sink1 ).size() );
    assertEquals( 10, asSet( flow, sink2 ).size() );
    }

  @Test
  public void testSplitComplex() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink1 = getPlatform().getTextFile( getOutputPath( "splitcomp1" ), SinkMode.REPLACE );
    Tap sink2 = getPlatform().getTextFile( getOutputPath( "splitcomp2" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    pipe = new Each( pipe, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", pipe ), new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    Pipe right = new Each( new Pipe( "right", pipe ), new Fields( "ip" ), new RegexFilter( ".*102.*" ) );

    Map sources = Cascades.tapsMap( "split", source );
    Map sinks = Cascades.tapsMap( Pipe.pipes( left, right ), Tap.taps( sink1, sink2 ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sinks, left, right );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 1, "right" );
    }

  @Test
  public void testSplitMultiple() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sinkLeft = getPlatform().getTextFile( getOutputPath( "left" ), SinkMode.REPLACE );
    Tap sinkRightLeft = getPlatform().getTextFile( getOutputPath( "rightleft" ), SinkMode.REPLACE );
    Tap sinkRightRight = getPlatform().getTextFile( getOutputPath( "rightright" ), SinkMode.REPLACE );

    Pipe head = new Pipe( "split" );

    head = new Each( head, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    head = new GroupBy( head, new Fields( "ip" ) );

    head = new Every( head, new Fields( "ip" ), new Count(), new Fields( "ip", "count" ) );

    head = new Each( head, new Fields( "ip" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( new Pipe( "left", head ), new Fields( "ip" ), new RegexFilter( ".*46.*" ) );

    Pipe right = new Each( new Pipe( "right", head ), new Fields( "ip" ), new RegexFilter( ".*102.*" ) );

    right = new GroupBy( right, new Fields( "ip" ) );

    Pipe rightLeft = new Each( new Pipe( "rightLeft", right ), new Fields( "ip" ), new Identity() );

    Pipe rightRight = new Each( new Pipe( "rightRight", right ), new Fields( "ip" ), new Identity() );

    Map sources = Cascades.tapsMap( "split", source );
    Map sinks = Cascades.tapsMap( Pipe.pipes( left, rightLeft, rightRight ), Tap.taps( sinkLeft, sinkRightLeft, sinkRightRight ) );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sinks, left, rightLeft, rightRight );

    flow.complete();

    validateLength( flow, 1, "left" );
    validateLength( flow, 1, "rightLeft" );
    validateLength( flow, 1, "rightRight" );
    }

  @Test
  public void testConcatenation() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getTextFile( inputFileLower );
    Tap sourceUpper = getPlatform().getTextFile( inputFileUpper );

    Tap source = new MultiSourceTap( sourceLower, sourceUpper );

    Function splitter = new RegexSplitter( new Fields( "num", "char" ), " " );

    // using null pos so all fields are written
    Tap sink = getPlatform().getTextFile( getOutputPath( "complexconcat" ), SinkMode.REPLACE );

    Pipe pipe = new Each( new Pipe( "concat" ), new Fields( "line" ), splitter );

    Pipe splice = new GroupBy( pipe, new Fields( "num" ) );

    Flow countFlow = getPlatform().getFlowConnector().connect( source, sink, splice );

    countFlow.complete();

    validateLength( countFlow, 10, null );
    }

  @Test
  public void testGeneratorAggregator() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip" ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new TestAggregator( new Fields( "count1" ), new Fields( "ip" ), new Tuple( "first1" ), new Tuple( "first2" ) ) );
    pipe = new Every( pipe, new TestAggregator( new Fields( "count2" ), new Fields( "ip" ), new Tuple( "second" ), new Tuple( "second2" ), new Tuple( "second3" ) ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "generatoraggregator" ), SinkMode.REPLACE );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8 * 2 * 3, null );
    }

  @Test
  public void testReplace() throws Exception
    {
    copyFromLocal(inputFileApache);

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( new Fields( "offset", "line" ), new Fields( "offset", "line" ), getOutputPath( "replace" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( 0 ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( "line" ), parser, Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new Identity( Fields.ARGS ), Fields.REPLACE );
    pipe = new Each( pipe, new Fields( "line" ), new Identity( new Fields( "line" ) ), Fields.REPLACE );

    pipe = new Each( pipe, new Debug( true ) );

    Flow flow = getPlatform().getFlowConnector( disableDebug() ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 10, 2, Pattern.compile( "^\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}$" ) );
    }

  @Test
  public void testSwap() throws Exception
    {
    copyFromLocal(inputFileApache);

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( new Fields( "offset", "line" ), new Fields( "count", "ipaddress" ), getOutputPath( "swap" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( "ip" ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( "line" ), parser, Fields.SWAP );
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Fields( "ip" ), new Count( new Fields( "count" ) ) );
    pipe = new Each( pipe, new Fields( "ip" ), new Identity( new Fields( "ipaddress" ) ), Fields.SWAP );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, 2, Pattern.compile( "^\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}$" ) );
    }

  @Test
  public void testNone() throws Exception
    {
    copyFromLocal(inputFileApache);

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( new Fields( "offset", "line" ), new Fields( "count", "ip" ), getOutputPath( "none" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "test" );

    Function parser = new RegexParser( new Fields( "ip" ), "^[^ ]*" );
    pipe = new Each( pipe, new Fields( "line" ), parser, Fields.ALL );
    pipe = new Each( pipe, new Fields( "line" ), new NoOp(), Fields.SWAP ); // declares Fields.NONE
    pipe = new GroupBy( pipe, new Fields( "ip" ) );
    pipe = new Every( pipe, new Fields( "ip" ), new Count( new Fields( "count" ) ) );
    pipe = new Each( pipe, Fields.NONE, new Insert( new Fields( "ipaddress" ), "1.2.3.4" ), Fields.ALL );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, 2, Pattern.compile( "^\\d+\\s\\d+\\s[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}\\.[\\d]{1,3}$" ) );
    }

  /**
   * this tests a merge on two pipes with the same source and name.
   *
   * @throws Exception
   */
  @Test
  public void testSplitSameSourceMergedSameName() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    // 46 192

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );
    Tap sink = getPlatform().getTextFile( getOutputPath( "splitsourcemergedsamename" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "split" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexFilter( "^68.*" ) );

    Pipe left = new Each( pipe, new Fields( "line" ), new RegexFilter( ".*46.*" ) );
    Pipe right = new Each( pipe, new Fields( "line" ), new RegexFilter( ".*102.*" ) );

    Pipe merged = new GroupBy( "merged", Pipe.pipes( left, right ), new Fields( "line" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, merged );

    flow.complete();

    validateLength( flow, 3 );
    }

  /**
   * Catches failure to properly resolve the grouping fields as incoming to the second group-by
   *
   * @throws Exception
   */
  @Test
  public void testGroupGroup() throws Exception
    {
    getPlatform().copyFromLocal( inputFileApache );

    Tap source = getPlatform().getTextFile( new Fields( "offset", "line" ), inputFileApache );

    Pipe pipe = new Pipe( "test" );

    pipe = new Each( pipe, new Fields( "line" ), new RegexParser( new Fields( "ip", String.class ), "^[^ ]*" ), new Fields( "ip" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ) );

    pipe = new Every( pipe, new Count(), new Fields( "ip", "count" ) );

    pipe = new GroupBy( pipe, new Fields( "ip" ), new Fields( "count" ) );

    Tap sink = getPlatform().getTextFile( getOutputPath( "groupgroup" ), SinkMode.REPLACE );

    Map<Object, Object> properties = getProperties();

    properties.put( "cascading.serialization.types.required", "true" );

    Flow flow = getPlatform().getFlowConnector( properties ).connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 8, null );
    }

  public static class LowerComparator implements Comparator<Comparable>, Hasher<Comparable>, Serializable
    {
    @Override
    public int compare( Comparable lhs, Comparable rhs )
      {
      return lhs.toString().toLowerCase().compareTo( rhs.toString().toLowerCase() );
      }

    @Override
    public int hashCode( Comparable value )
      {
      if( value == null )
        return 0;

      return value.toString().toLowerCase().hashCode();
      }
    }

  @Test
  public void testGroupByInsensitive() throws Exception
    {
    getPlatform().copyFromLocal( inputFileLower );
    getPlatform().copyFromLocal( inputFileUpper );

    Tap sourceLower = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileLower );
    Tap sourceUpper = getPlatform().getDelimitedFile( new Fields( "num", "char" ), " ", inputFileUpper );

    Map sources = new HashMap();

    sources.put( "lower", sourceLower );
    sources.put( "upper", sourceUpper );

    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "insensitivegrouping" + NONDETERMINISTIC ), SinkMode.REPLACE );

    Pipe pipeLower = new Pipe( "lower" );
    Pipe pipeUpper = new Pipe( "upper" );

    Pipe merge = new Merge( pipeLower, pipeUpper );

    Fields charFields = new Fields( "char" );
    charFields.setComparator( "char", new LowerComparator() );

    Pipe splice = new GroupBy( "groupby", merge, charFields );

    splice = new Every( splice, new Fields( "char" ), new Count() );

    Flow flow = getPlatform().getFlowConnector().connect( sources, sink, splice );

    flow.complete();

    // we can't guarantee if the grouping key will be upper or lower
    validateLength( flow, 5, 1, Pattern.compile( "^\\w+\\s2$" ) );
    }
  }
