/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.mr.gather;

import java.io.IOException;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.gather.KijiGatherer;
import org.kiji.mapreduce.gather.GathererContext;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiDataRequestBuilder;
import org.kiji.schema.KijiRowData;

/**
 * Example gatherer.
 *
 * <p>
 * This gatherer emits a pair of the input text and the number 1. This is a pattern
 * often used while counting.
 * </p>
 * <p>
 * Change the <code>getDataRequest()</code> method to change the column this gatherer uses as input.
 * Change the <code>gather()</code> method to change what kind of output this gatherer produces.
 * </p>
 * <p>
 * This example gatherer implements both AvroKeyWriter and AvroValueWriter, but only the Key is
 * an Avro class.  The AvroValueWriter.getAvroValueWriterSchema() method is a dummy implementation
 * that you will have to edit if your output value is an Avro record.  If your keys or values are
 * not Avro classes, you can remove the AvroKeyWriter and AvroValueWriter interfaces, respectively.
 * </p>
 */
public class WordCountGatherer extends KijiGatherer<Text, LongWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(WordCountGatherer.class);

  /** The family:qualifier of the column to read in. **/
  private static final String COLUMN_FAMILY = "info";
  private static final String COLUMN_QUALIFIER = "name";

  /** Only keep one LongWritable object around to reduce the chance of a garbage collection pause.*/
  private static final LongWritable ONE = new LongWritable(1);
  private static final Text KEY = new Text();

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(GathererContext<Text, LongWritable> context) throws IOException {
    super.setup(context); // Any time you override setup, call super.setup(context);
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // This method is how we specify which columns in each row the gatherer operates on.
    // In this case, we need all versions of the info:track_plays column.
    final KijiDataRequestBuilder builder = KijiDataRequest.builder();
    builder.newColumnsDef()
        .withMaxVersions(HConstants.ALL_VERSIONS) // Retrieve all versions.
        .add(COLUMN_FAMILY, COLUMN_QUALIFIER);
    return builder.build();
  }

  /** {@inheritDoc} */
  @Override
  public void gather(KijiRowData row, GathererContext<Text, LongWritable> context)
      throws IOException {
    // TODO: Implement the body of your gather method here.
    // This gatherer emits a pair of the key and the number 1, which is a pattern often used
    // when counting.
    NavigableMap<Long, CharSequence> counts = row.getValues(COLUMN_FAMILY, COLUMN_QUALIFIER);
    for (CharSequence key : counts.values()) {
      LOG.info("Read out data! user = " + key);
      KEY.set(key.toString());
      context.write(KEY, ONE);
    }

  }
}
