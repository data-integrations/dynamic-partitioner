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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.cdap.etl.common.BasicArguments;
import co.cask.cdap.etl.common.DefaultMacroEvaluator;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.MacroParser;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Avro records to a {@link PartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("AvroDynamicPartitionedDataset")
@Description("Sink for a PartitionedFileSet that writes data in Avro format and uses a dynamic partition key.")
public class AvroDynamicPartitionedDatasetSink extends
  PartitionedFileSetSink<AvroKey<GenericRecord>, NullWritable> {
  public static final String NAME = "AvroDynamicPartitionedDataset";

  private static final Logger LOG = LoggerFactory.getLogger(AvroDynamicPartitionedDatasetSink.class);

  private StructuredToAvroTransformer recordTransformer;
  private final PartitionedFileSetSinkConfig config;

  public AvroDynamicPartitionedDatasetSink(PartitionedFileSetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    super.prepareRun(context);
    Map<String, String> sinkArgs = getAdditionalPFSArguments();
    DynamicPartitioner.PartitionWriteOption writeOption = config.partitionWriteOption == null ?
      DynamicPartitioner.PartitionWriteOption.CREATE :
      DynamicPartitioner.PartitionWriteOption.valueOf(config.partitionWriteOption);
    PartitionedFileSetArguments.setDynamicPartitioner
      (sinkArgs, AvroDynamicPartitionedDatasetSink.FieldValueDynamicPartitioner.class, writeOption);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema, config.fieldNames);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  @Override
  protected void addPartitionedFileSetProperties(PartitionedFileSetProperties.Builder properties) {
    FileSetUtil.configureAvroFileSet(config.schema, properties);
    properties.addAll(FileSetUtil.getAvroCompressionConfiguration(config.compressionCodec, config.schema, true));
  }

  @Override
  protected DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                   Map.Entry<Schema, String> outputSchemaPair) {
    PartitionedFileSetProperties.Builder properties = PartitionedFileSetProperties.builder();
    addPartitionedFileSetProperties(properties);
    return properties
      .setPartitioning(partitioning)
      .setBasePath(config.getNonNullBasePath())
      .setTableProperty("avro.schema.literal", outputSchemaPair.getKey().toString())
      .setExploreSchema(outputSchemaPair.getValue().substring(1, outputSchemaPair.getValue().length() - 1))
      .add(DatasetProperties.SCHEMA, config.schema)
      .build();
  }

  /**
   * Dynamic partitioner that creates partitions based on a list of fields in each record.
   */
  public static final class FieldValueDynamicPartitioner
    extends DynamicPartitioner<AvroKey<GenericRecord>, NullWritable> {
    private String[] fieldNames;

    @Override
    public void initialize(MapReduceTaskContext mapReduceTaskContext) {
      // Need a better way to do this. [CDAP-7058]
      String rawFieldNames = mapReduceTaskContext
        .getPluginProperties(AvroDynamicPartitionedDatasetSink.NAME)
        .getProperties().get(PartitionedFileSetSinkConfig.FIELD_NAMES_PROPERTY_KEY);

      // Need to use macro parser here. [CDAP-11960]
      BasicArguments basicArguments = new BasicArguments(mapReduceTaskContext.getRuntimeArguments());
      MacroEvaluator macroEvaluator = new DefaultMacroEvaluator(basicArguments,
                                                                mapReduceTaskContext.getLogicalStartTime(),
                                                                mapReduceTaskContext,
                                                                mapReduceTaskContext.getNamespace());
      MacroParser macroParser = new MacroParser(macroEvaluator);
      fieldNames = macroParser.parse(rawFieldNames).split(",");
    }

    @Override
    public PartitionKey getPartitionKey(AvroKey<GenericRecord> key, NullWritable value) {
      PartitionKey.Builder keyBuilder = PartitionKey.builder();
      for (int i = 0; i < fieldNames.length; i++) {
        // discard leading and trailing white spaces in the partition name
        String columnValue = String.valueOf(key.datum().get(fieldNames[i])).trim();
        if (columnValue.contains(Path.SEPARATOR)) {
          // having path separator as partition column value creates additional partition layers. CDAP expects the
          // number of partition layers to be the same as the number of partition field names.
          throw new IllegalArgumentException(String.format("Partition column value must not include path separator %s",
                                                           Path.SEPARATOR));
        } else {
          keyBuilder.addStringField(fieldNames[i], columnValue);
        }
      }
      return keyBuilder.build();
    }
  }

}
