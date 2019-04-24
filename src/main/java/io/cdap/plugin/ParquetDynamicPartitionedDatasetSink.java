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

package io.cdap.plugin;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.DatasetManagementException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.api.dataset.lib.DynamicPartitioner;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.dataset.lib.PartitionKey;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSet;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetArguments;
import io.cdap.cdap.api.dataset.lib.PartitionedFileSetProperties;
import io.cdap.cdap.api.dataset.lib.Partitioning;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.batch.BatchRuntimeContext;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.common.Constants;
import io.cdap.plugin.common.FileSetUtil;
import io.cdap.plugin.common.Schemas;
import io.cdap.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@link BatchSink} to write Parquet records to a {@link PartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(ParquetDynamicPartitionedDatasetSink.NAME)
@Description("Sink for a PartitionedFileSet that writes data in Parquet format and uses a dynamic partition key.")
public class ParquetDynamicPartitionedDatasetSink extends PartitionedFileSetSink<Void, GenericRecord> {
  public static final String NAME = "ParquetDynamicPartitionedDataset";
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final PartitionedFileSetSinkConfig config;
  private StructuredToAvroTransformer recordTransformer;
  private String stageName;

  public ParquetDynamicPartitionedDatasetSink(PartitionedFileSetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException {
    super.prepareRun(context);
    Map<String, String> sinkArgs = new HashMap<>();
    DynamicPartitioner.PartitionWriteOption writeOption =
      config.appendToPartition == null || "No".equals(config.appendToPartition) ?
        DynamicPartitioner.PartitionWriteOption.CREATE :
        DynamicPartitioner.PartitionWriteOption.CREATE_OR_APPEND;
    PartitionedFileSetArguments.setDynamicPartitioner(sinkArgs, ParquetDynamicPartitioner.class, writeOption);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);

    Schema modified = Schemas.addField(config.getSchema(), Constants.STAGE_FIELD);
    Map<String, Object> constants = new HashMap<>();
    constants.put(Constants.STAGE_FIELD_NAME, context.getStageName());

    recordTransformer = new StructuredToAvroTransformer(modified.toString(), constants);
    this.stageName = context.getStageName();
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  @Override
  protected DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                   Map.Entry<Schema, String> outputSchemaPair) {
    PartitionedFileSetProperties.Builder properties = PartitionedFileSetProperties.builder();

    FileSetUtil.configureParquetFileSet(config.schema, properties);
    properties.addAll(FileSetUtil.getParquetCompressionConfiguration(config.compressionCodec, config.schema, true));
    return properties
      .setPartitioning(partitioning)
      .setBasePath(config.getNonNullBasePath())
      .setExploreSchema(outputSchemaPair.getValue().substring(1, outputSchemaPair.getValue().length() - 1))
      .build();
  }

  /**
   * Dynamic partitioner that creates partitions based on a list of fields in each record.
   */
  public static final class ParquetDynamicPartitioner extends FieldValueDynamicPartitioner<Void, GenericRecord> {

    @Override
    public PartitionKey getPartitionKey(Void key, GenericRecord value) {
      String[] fieldNames = stageFieldNames.get(value.get(Constants.STAGE_FIELD_NAME));
      PartitionKey.Builder keyBuilder = PartitionKey.builder();
      for (int i = 0; i < fieldNames.length; i++) {
        // discard leading and trailing white spaces in the partition name
        String columnValue = String.valueOf(value.get(fieldNames[i])).trim();
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
