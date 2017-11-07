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
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.action.SettableArguments;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.common.Constants;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.Schemas;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;

import java.util.HashMap;
import java.util.Map;

/**
 * A {@link BatchSink} to write Avro records to a {@link PartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(AvroDynamicPartitionedDatasetSink.NAME)
@Description("Sink for a PartitionedFileSet that writes data in Avro format and uses a dynamic partition key.")
public class AvroDynamicPartitionedDatasetSink extends PartitionedFileSetSink<AvroKey<GenericRecord>, NullWritable> {
  public static final String NAME = "AvroDynamicPartitionedDataset";
  private final PartitionedFileSetSinkConfig config;
  private StructuredToAvroTransformer recordTransformer;

  public AvroDynamicPartitionedDatasetSink(PartitionedFileSetSinkConfig config) {
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
    PartitionedFileSetArguments.setDynamicPartitioner(sinkArgs, AvroDynamicPartitioner.class, writeOption);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);

    Schema modified = Schemas.addField(config.getSchema(), Constants.STAGE_FIELD);
    Map<String, Object> constants = new HashMap<>();
    constants.put(Constants.STAGE_FIELD_NAME, context.getStageName());
    recordTransformer = new StructuredToAvroTransformer(modified.toString(), constants);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  @Override
  protected DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                   Map.Entry<Schema, String> outputSchemaPair) {
    PartitionedFileSetProperties.Builder properties = PartitionedFileSetProperties.builder();
    FileSetUtil.configureAvroFileSet(config.schema, properties);
    properties.addAll(FileSetUtil.getAvroCompressionConfiguration(config.compressionCodec, config.schema, true));
    return properties
      .setPartitioning(partitioning)
      .setBasePath(config.getNonNullBasePath())
      .setTableProperty("avro.schema.literal", outputSchemaPair.getKey().toString())
      .setExploreSchema(outputSchemaPair.getValue().substring(1, outputSchemaPair.getValue().length() - 1))
      .build();
  }

  /**
   * Dynamic partitioner that creates partitions based on a list of fields in each record.
   */
  public static final class AvroDynamicPartitioner extends
    FieldValueDynamicPartitioner<AvroKey<GenericRecord>, NullWritable> {

    @Override
    public PartitionKey getPartitionKey(AvroKey<GenericRecord> key, NullWritable value) {
      String[] fieldNames = stageFieldNames.get(key.datum().get(Constants.STAGE_FIELD_NAME));
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
