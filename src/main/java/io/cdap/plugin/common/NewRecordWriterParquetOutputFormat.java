/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;

/**
 * Parquet outputFormat to always return a new RecordWriter object created with a new AvroParquetOutputFormat.
 * This is a workaround for [CDAP-12823]
 *
 * @param <T> the type of the materialized records
 */
public class NewRecordWriterParquetOutputFormat<T> extends AvroParquetOutputFormat<T> {

  @Override
  public RecordWriter<Void, T> getRecordWriter(Configuration conf, Path file, CompressionCodecName codec)
    throws IOException, InterruptedException {
    return new AvroParquetOutputFormat<T>().getRecordWriter(conf, file, codec);
  }
}
