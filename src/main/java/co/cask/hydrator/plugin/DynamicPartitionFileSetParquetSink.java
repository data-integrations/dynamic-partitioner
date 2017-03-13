/*
 * Copyright © 2016 Cask Data, Inc.
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
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write Parquet records to a {@link PartitionedFileSet}.
 */
@Plugin(type = "batchsink")
@Name("DynamicPFSParquet")
@Description("Sink for a PartitionedFileSet that writes data in Parquet format and uses a dynamic partition key.")
public class DynamicPartitionFileSetParquetSink extends
  PartitionedFileSetSink<Void, GenericRecord> {
  public static final String NAME = "DynamicPFSParquet";

  private static final Logger LOG = LoggerFactory.getLogger(DynamicPartitionFileSetParquetSink.class);
  private static final String SCHEMA_DESC = "The Parquet schema of the record being written to the Sink as a JSON " +
    "Object.";
  private static final String FIELD_DESC = "The fields to be used for the partitions as comma separated values.";

  private StructuredToAvroTransformer recordTransformer;
  private final DynamicPartitionParquetSinkConfig config;

  public DynamicPartitionFileSetParquetSink(DynamicPartitionParquetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) {
    Map<String, String> sinkArgs = getAdditionalPFSArguments();
    PartitionedFileSetArguments.setDynamicPartitioner(sinkArgs, FieldValueDynamicPartitioner.class);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    String pfsName = config.name;
    String basePath = config.basePath == null ? pfsName : config.basePath;

    // parse to make sure it's valid
    new org.apache.avro.Schema.Parser().parse(config.schema);
    String hiveSchema;
    Schema parsedSchema;
    try {
      parsedSchema = Schema.parseJson(config.schema);
      hiveSchema = HiveSchemaConverter.toHiveSchema(parsedSchema);
    } catch (UnsupportedTypeException | IOException e) {
      throw new RuntimeException("Error: Schema is not valid ", e);
    }

    Partitioning.Builder partitionBuilder = Partitioning.builder();
    String[] partitionFields = config.fieldNames.split(",");
    for (int i = 0; i < partitionFields.length; i++) {
      partitionBuilder.addStringField(partitionFields[i]);
    }


    List<Schema.Field> fields = new ArrayList<>();
    List<String> toRemove = Arrays.asList(config.fieldNames.split(","));
    for (Schema.Field field : parsedSchema.getFields()) {
      if (!toRemove.contains(field.getName())) {
        fields.add(field);
      }
    }
    String outputHiveSchema = null;
    try {
      outputHiveSchema = HiveSchemaConverter.toHiveSchema(Schema.recordOf("output", fields));
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("Error: Schema is not valid ", e);
    }

    PartitionedFileSetProperties.Builder properties = PartitionedFileSetProperties.builder();
    addPartitionedFileSetProperties(properties);

    pipelineConfigurer.createDataset(pfsName, PartitionedFileSet.class.getName(),
                                     properties
                                       .setPartitioning(partitionBuilder.build())
                                       .setBasePath(basePath)
                                       .setExploreSchema(outputHiveSchema.substring(1, outputHiveSchema.length() - 1))
                                       .add(DatasetProperties.SCHEMA, config.schema)
                                       .build());
  }

  @Override
  protected Map<String, String> getAdditionalPFSArguments() {
    Map<String, String> args = new HashMap<>();
    args.put(FileSetProperties.OUTPUT_PROPERTIES_PREFIX + "parquet.avro.schema", config.schema);
    return args;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema, config.fieldNames);
  }

  @Override
  protected void addPartitionedFileSetProperties(PartitionedFileSetProperties.Builder properties) {
    FileSetUtil.configureParquetFileSet(config.schema, properties);
    properties.addAll(FileSetUtil.getParquetCompressionConfiguration(config.compressionCodec, config.schema, true));
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  /**
   * Config for DynamicPartitionFileSetParquetSink
   */
  public static class DynamicPartitionParquetSinkConfig extends PartitionedFileSetSinkConfig {
    public static final String FIELD_NAMES_PROPERTY_KEY = "fieldNames";

    @Description(SCHEMA_DESC)
    private String schema;
    @Description(FIELD_DESC)
    private String fieldNames;

    public DynamicPartitionParquetSinkConfig(String name, String schema, String fieldNames,
                                             @Nullable String basePath, @Nullable String compressionCodec) {
      super(name, basePath, compressionCodec);
      this.schema = schema;
      this.fieldNames = fieldNames;
    }
  }

  /**
   * Dynamic partitioner that creates partitions based on a list of fields in each record.
   */
  public static final class FieldValueDynamicPartitioner extends DynamicPartitioner<Void, GenericRecord> {
    private String[] fieldNames;

    @Override
    public void initialize(MapReduceTaskContext mapReduceTaskContext) {
      if (mapReduceTaskContext.getPluginProperties(DynamicPartitionFileSetParquetSink.NAME) == null) {
        throw new IllegalArgumentException("Could not find a plugin with the name: " +
                                             DynamicPartitionFileSetParquetSink.NAME + " in the list of plugins.");
      }
      // Need a better way to do this. [CDAP-7058]
      fieldNames = mapReduceTaskContext
        .getPluginProperties(DynamicPartitionFileSetParquetSink.NAME)
        .getProperties().get(DynamicPartitionParquetSinkConfig.FIELD_NAMES_PROPERTY_KEY).split(",");
    }

    @Override
    public PartitionKey getPartitionKey(Void key, GenericRecord value) {
      PartitionKey.Builder keyBuilder = PartitionKey.builder();
      for (int i = 0; i < fieldNames.length; i++) {
        if (value.get(fieldNames[i]) == null) {
          // This call will result in an exception but there's nothing else I can do. [CDAP-7053]
          keyBuilder.addStringField(fieldNames[i], null);
        } else {
          keyBuilder.addStringField(fieldNames[i], String.valueOf(value.get(fieldNames[i])));
        }
      }
      return keyBuilder.build();
    }
  }

}
