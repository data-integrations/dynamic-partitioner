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

package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.action.SettableArguments;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * PFS Batch Sink class that stores sink data
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class PartitionedFileSetSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  protected final PartitionedFileSetSinkConfig partitionedSinkConfig;

  protected PartitionedFileSetSink(PartitionedFileSetSinkConfig partitionedSinkConfig) {
    this.partitionedSinkConfig = partitionedSinkConfig;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    Schema inputSchema = context.getInputSchema();
    partitionedSinkConfig.validate(inputSchema);

    // if macros were provided and the dataset doesn't exist, create dataset now
    if (!context.datasetExists(partitionedSinkConfig.name)) {
      // validate field, schema and get output hive schema
      Map.Entry<Schema, String> outputSchemaPair = partitionedSinkConfig.getOutputSchema();

      // validate fields and get partitionBuilder
      Partitioning partitioning = partitionedSinkConfig.getPartitioning(inputSchema);

      // create properties and dataset
      DatasetProperties properties = getDatasetProperties(partitioning, outputSchemaPair);
      context.createDataset(partitionedSinkConfig.name, PartitionedFileSet.class.getName(), properties);
    }

    SettableArguments args = context.getArguments();
    Map<String, String> stageFieldnames = new HashMap<>();
    if (args.has("fieldnames")) {
      stageFieldnames = GSON.fromJson(args.get("fieldnames"), MAP_TYPE);
    }
    stageFieldnames.put(context.getStageName(), partitionedSinkConfig.fieldNames);
    args.set("fieldnames", GSON.toJson(stageFieldnames));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    partitionedSinkConfig.validate(inputSchema);

    // configure right away if no macros are enabled
    if (!partitionedSinkConfig.containsMacro("name") && !partitionedSinkConfig.containsMacro("basePath") &&
      !partitionedSinkConfig.containsMacro("schema") && !partitionedSinkConfig.containsMacro("fieldNames")) {
      // validate field, schema and get output hive schema
      Map.Entry<Schema, String> outputSchemaPair = partitionedSinkConfig.getOutputSchema();

      // validate fields and get partitionBuilder
      Partitioning partitioning = partitionedSinkConfig.getPartitioning(inputSchema);

      // create properties and dataset
      DatasetProperties properties = getDatasetProperties(partitioning, outputSchemaPair);
      pipelineConfigurer.createDataset(partitionedSinkConfig.name, PartitionedFileSet.class.getName(), properties);
    }
  }

  protected abstract DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                            Map.Entry<Schema, String> outputSchemaPair);
}
