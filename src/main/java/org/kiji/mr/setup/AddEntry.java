/**
 * (c) Copyright 2012 WibiData, Inc.
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

package org.kiji.mr.setup;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.kiji.schema.EntityId;
import org.kiji.schema.KConstants;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiTable;
import org.kiji.schema.KijiTableNotFoundException;
import org.kiji.schema.KijiTableWriter;
import org.kiji.schema.KijiURI;
import org.kiji.schema.util.ResourceUtils;

/**
 * Put some data into the toy table.
 */
public class AddEntry extends Configured implements Tool {
  /** Name of the table to use.  Must match DDL file. */
  public static final String TABLE_NAME = "users";

  /**
   * Run the entry addition system. Asks the user for values for all fields
   * and then fills them in.
   *
   * @param args Command line arguments; this is expected to be empty.
   * @return Exit status code for the application; 0 indicates success.
   * @throws IOException If an error contacting Kiji occurs.
   * @throws InterruptedException If the process is interrupted while performing I/O.
   */
  @Override
  public int run(String[] args) throws IOException, InterruptedException {

    KijiURI kijiURI = KijiURI.newBuilder(args[0]).build();

    Kiji kiji = null;
    KijiTable table = null;
    KijiTableWriter writer = null;
    try {
      // Connect to Kiji and open the table.
      kiji = Kiji.Factory.open(kijiURI, getConf());
      table = kiji.openTable(TABLE_NAME);
      writer = table.openTableWriter();
      EntityId eid;

      // Insert a bunch of data!!!!!!!
      eid = table.getEntityId("a");
      writer.put(eid, "info", "name", "Bob Marley");
      writer.put(eid, "info", "state", "CA");

      eid = table.getEntityId("b");
      writer.put(eid, "info", "name", "Bob Dylan");
      writer.put(eid, "info", "state", "MA");

      eid = table.getEntityId("c");
      writer.put(eid, "info", "name", "Jazzy B");
      writer.put(eid, "info", "state", "Canada");

    } catch (KijiTableNotFoundException e) {
      System.out.println("Could not find Kiji table: " + TABLE_NAME);
      return 1;
    } finally {
      // Safely free up resources by closing in reverse order.
      ResourceUtils.closeOrLog(writer);
      ResourceUtils.releaseOrLog(table);
      ResourceUtils.releaseOrLog(kiji);
    }

    return 0;
  }

  /**
   * Program entry point. Terminates the application without returning.
   *
   * @param args The arguments from the command line. May start with Hadoop "-D" options.
   * @throws Exception If the application encounters an exception.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new AddEntry(), args));
  }
}
