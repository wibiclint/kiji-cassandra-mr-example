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

package org.kiji.mr.reduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.kiji.annotations.ApiAudience;
import org.kiji.mapreduce.KijiReducer;

/**
 * This MapReduce reducer will pass through all of the input key-value pairs unchanged.
 *
 * <p>
 * The key and value types here are both the Avro record ExampleRecord.
 * </p>
 *
 * <p>
 * To implement your own Reducer, extend KijiReducer with the type parameters for your input
 * and output keys and values, and override the reduce method.  You can also override the
 * <code>setup</code> and <code>cleanup</code> methods if necessary.
 * </p>
 *
 * <p>
 * If your any of your input or output key or value types aren't Avro records,
 * you don't need to implement the AvroKeyReader, AvroKeyWriter, etc, interfaces.
 * </p>
 */
@ApiAudience.Public
public final class WordCountReducer
    extends KijiReducer<Text, LongWritable, Text, LongWritable> {

  /** {@inheritDoc} */
  @Override
  protected void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (LongWritable val : values) {
      sum += val.get();
    }

    context.write(key, new LongWritable(sum));
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return LongWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}
