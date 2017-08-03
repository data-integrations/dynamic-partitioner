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
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * PFS Batch Sink class that stores sink data
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class PartitionedFileSetSink<KEY_OUT, VAL_OUT>
  extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionedFileSetSink.class);

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

  /**
   * @return any additional properties that need to be set for the sink. For example, avro sink requires
   *         setting some schema output key.
   */
  protected Map<String, String> getAdditionalPFSArguments() {
    return new HashMap<>();
  }

  protected DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                   Map.Entry<Schema, String> outputSchemaPair) {
    return PartitionedFileSetProperties.builder().build();
  }

  /**
   * Set file set specific properties, such as input/output format and explore properties.
   */
  protected abstract void addPartitionedFileSetProperties(PartitionedFileSetProperties.Builder properties);
}
