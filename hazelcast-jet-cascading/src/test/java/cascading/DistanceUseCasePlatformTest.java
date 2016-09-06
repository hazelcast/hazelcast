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
import java.util.List;

import cascading.assembly.EuclideanDistance;
import cascading.assembly.PearsonDistance;
import cascading.assembly.SortElements;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.operation.AggregatorCall;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.aggregator.Sum;
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
import cascading.tuple.TupleEntry;
import org.junit.Test;

import static data.InputData.inputFileCritics;

public class DistanceUseCasePlatformTest extends PlatformTestCase implements Serializable
  {
  public DistanceUseCasePlatformTest()
    {
    super( false );
    }

  /**
   * Calculate the euclidean distance of the people in the critics.txt file
   *
   * @throws java.io.IOException
   */
  @Test
  public void testEuclideanDistance() throws Exception
    {
    getPlatform().copyFromLocal( inputFileCritics );

    Tap source = getPlatform().getTextFile( inputFileCritics );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "euclidean/long" ), SinkMode.REPLACE );

    Pipe pipe = new Pipe( "euclidean" );

    // unknown number of elements
    pipe = new Each( pipe, new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), new Fields( 0 ), 2 ) );

    // name and rate against others of same movie
    pipe = new CoGroup( pipe, new Fields( "movie" ), 1, new Fields( "name1", "movie", "rate1", "name2", "movie2", "rate2" ) );

    // remove useless fields
    pipe = new Each( pipe, new Fields( "movie", "name1", "rate1", "name2", "rate2" ), new Identity() );

    // remove lines if the names are the same
    pipe = new Each( pipe, new RegexFilter( "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*", true ) );

    // transpose values in fields by natural sort order
    pipe = new Each( pipe, new SortElements( new Fields( "name1", "rate1" ), new Fields( "name2", "rate2" ) ) );

    // unique the pipe
    pipe = new GroupBy( pipe, Fields.ALL );
    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    // calculate square of diff
    Function sqDiff = new Identity( new Fields( "score" ) )
    {
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      TupleEntry input = functionCall.getArguments();
      functionCall.getOutputCollector().add( new Tuple( Math.pow( input.getTuple().getDouble( 0 ) - input.getTuple().getDouble( 1 ), 2 ) ) );
      }
    };

    // out: movie, name1, rate1, name2, rate2, score
    pipe = new Each( pipe, new Fields( "rate1", "rate2" ), sqDiff, Fields.ALL );

    // sum and sqr for each name pair
    pipe = new GroupBy( pipe, new Fields( "name1", "name2" ) );

    Sum distance = new Sum( new Fields( "distance" ) )
    {
    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      Tuple tuple = super.getResult( aggregatorCall );

      aggregatorCall.getOutputCollector().add( new Tuple( 1 / ( 1 + tuple.getDouble( 0 ) ) ) );
      }
    };

    pipe = new Every( pipe, new Fields( "score" ), distance, new Fields( "name1", "name2", "distance" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 21 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "GeneSeymour\tLisaRose\t0.14814814814814814" ) ) );
    }

  /**
   * Calculate the euclidean distance of the people in the critics.txt file
   *
   * @throws java.io.IOException
   */
  @Test
  public void testEuclideanDistanceShort() throws Exception
    {
    getPlatform().copyFromLocal( inputFileCritics );

    Tap source = getPlatform().getTextFile( inputFileCritics );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "euclidean/short" ), SinkMode.REPLACE );

    // unknown number of elements
    Pipe pipe = new Each( "euclidean", new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), Fields.FIRST, 2 ) );

    // name and rate against others of same movie
    pipe = new CoGroup( pipe, new Fields( "movie" ), 1, new Fields( "name1", "movie", "rate1", "name2", "movie2", "rate2" ) );

    // remove useless fields
    pipe = new Each( pipe, new Fields( "movie", "name1", "rate1", "name2", "rate2" ), new Identity() );

    // remove lines if the names are the same
    pipe = new Each( pipe, new RegexFilter( "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*", true ) );

    // transpose values in fields by natural sort order
    pipe = new Each( pipe, new SortElements( new Fields( "name1", "rate1" ), new Fields( "name2", "rate2" ) ) );

    // unique the pipe
    pipe = new GroupBy( pipe, Fields.ALL );

    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    // calculate square of diff
    Function sqDiff = new Identity( new Fields( "score" ) )
    {
    public void operate( FlowProcess flowProcess, FunctionCall functionCall )
      {
      TupleEntry input = functionCall.getArguments();
      functionCall.getOutputCollector().add( new Tuple( Math.pow( input.getTuple().getDouble( 0 ) - input.getTuple().getDouble( 1 ), 2 ) ) );
      }
    };

    // out: movie, name1, rate1, name2, rate2, score
    pipe = new Each( pipe, new Fields( "rate1", "rate2" ), sqDiff, Fields.ALL );

    // sum and sqr for each name pair
    pipe = new GroupBy( pipe, new Fields( "name1", "name2" ) );

    Sum distance = new Sum( new Fields( "distance" ) )
    {
    public void complete( FlowProcess flowProcess, AggregatorCall aggregatorCall )
      {
      Tuple tuple = super.getResult( aggregatorCall );

      aggregatorCall.getOutputCollector().add( new Tuple( 1 / ( 1 + tuple.getDouble( 0 ) ) ) );
      }
    };

    pipe = new Every( pipe, new Fields( "score" ), distance, new Fields( "name1", "name2", "distance" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 21 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "GeneSeymour\tLisaRose\t0.14814814814814814" ) ) );
    }

  @Test
  public void testEuclideanDistanceComposite() throws Exception
    {
    getPlatform().copyFromLocal( inputFileCritics );

    Tap source = getPlatform().getTextFile( inputFileCritics );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "euclidean/composite" ), SinkMode.REPLACE );

    // unknown number of elements
    Pipe pipe = new Each( "euclidean", new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), Fields.FIRST, 2 ) );

    // name and rate against others of same movie
    pipe = new EuclideanDistance( pipe, new Fields( "name", "movie", "rate" ), new Fields( "name1", "name2", "distance" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 21 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "GeneSeymour\tLisaRose\t0.14814814814814814" ) ) );
    }

  @Test
  public void testPearsonDistanceComposite() throws Exception
    {
    getPlatform().copyFromLocal( inputFileCritics );

    Tap source = getPlatform().getTextFile( inputFileCritics );
    Tap sink = getPlatform().getTextFile( new Fields( "line" ), getOutputPath( "pearson/composite" ), SinkMode.REPLACE );

    // unknown number of elements
    Pipe pipe = new Each( "pearson", new Fields( "line" ), new RegexSplitter( "\t" ) );

    // break not names and movies
    pipe = new Each( pipe, new UnGroup( new Fields( "name", "movie", "rate" ), Fields.FIRST, 2 ) );

    // name and rate against others of same movie
    pipe = new PearsonDistance( pipe, new Fields( "name", "movie", "rate" ), new Fields( "name1", "name2", "distance" ) );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, pipe );

    flow.complete();

    validateLength( flow, 21 );

    List<Tuple> results = getSinkAsList( flow );

    assertTrue( results.contains( new Tuple( "GeneSeymour\tLisaRose\t0.39605901719066977" ) ) );
    }
  }
