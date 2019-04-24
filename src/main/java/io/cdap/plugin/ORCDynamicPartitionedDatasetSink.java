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

package io.cdap.plugin;

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
import io.cdap.plugin.common.StructuredToOrcTransformer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.mapred.OrcStruct;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write ORC records to a {@link PartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(ORCDynamicPartitionedDatasetSink.NAME)
@Description("Sink for a PartitionedFileSet that writes data in ORC format and uses a dynamic partition key.")
public class ORCDynamicPartitionedDatasetSink extends PartitionedFileSetSink<NullWritable, OrcStruct> {
  public static final String NAME = "ORCDynamicPartitionedDataset";
  private static final String ORC_COMPRESS = "orc.compress";
  private static final String SNAPPY_CODEC = "SNAPPY";
  private static final String ZLIB_CODEC = "ZLIB";
  private static final String COMPRESS_SIZE = "orc.compress.size";
  private static final String ROW_INDEX_STRIDE = "orc.row.index.stride";
  private static final String CREATE_INDEX = "orc.create.index";
  private StructuredToOrcTransformer recordTransformer;
  private final ORCDynamicPartitionedDatasetSinkConfig config;

  public ORCDynamicPartitionedDatasetSink(ORCDynamicPartitionedDatasetSinkConfig config) {
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
    PartitionedFileSetArguments.setDynamicPartitioner(sinkArgs, OrcDynamicPartitioner.class, writeOption);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    Schema modified = Schemas.addField(config.getSchema(), Constants.STAGE_FIELD);
    Map<String, Object> constants = new HashMap<>();
    constants.put(Constants.STAGE_FIELD_NAME, context.getStageName());
    recordTransformer = new StructuredToOrcTransformer(modified, constants);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, OrcStruct>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), recordTransformer.transform(input)));
  }

  @Override
  protected DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                   Map.Entry<Schema, String> outputSchemaPair) {
    PartitionedFileSetProperties.Builder properties = PartitionedFileSetProperties.builder();
    FileSetUtil.configureORCFileSet(config.schema, properties);
    if (config.compressionCodec != null && !config.compressionCodec.equalsIgnoreCase("None")) {
      switch (config.compressionCodec.toUpperCase()) {
        case SNAPPY_CODEC:
          properties.setOutputProperty(ORC_COMPRESS, SNAPPY_CODEC);
          break;
        case ZLIB_CODEC:
          properties.setOutputProperty(ORC_COMPRESS, ZLIB_CODEC);
          break;
        default:
          throw new IllegalArgumentException("Unsupported compression codec " + config.compressionCodec);
      }
      if (config.compressionChunkSize != null) {
        properties.setOutputProperty(COMPRESS_SIZE, config.compressionChunkSize.toString());
      }
      if (config.stripeSize != null) {
        properties.setOutputProperty(COMPRESS_SIZE, config.stripeSize.toString());
      }
      if (config.indexStride != null) {
        properties.setOutputProperty(ROW_INDEX_STRIDE, config.indexStride.toString());
      }
      if (config.createIndex != null) {
        properties.setOutputProperty(CREATE_INDEX, config.indexStride.toString());
      }
    }
    return properties
      .setPartitioning(partitioning)
      .setBasePath(config.getNonNullBasePath())
      .setExploreSchema(outputSchemaPair.getValue().substring(1, outputSchemaPair.getValue().length() - 1))
      .build();
  }

  /**
   * Config for ORCDynamicPartitionedDatasetSink
   */
  public static class ORCDynamicPartitionedDatasetSinkConfig extends PartitionedFileSetSinkConfig {

    @Nullable
    @Description("Number of bytes in each compression chunk.")
    private Long compressionChunkSize;

    @Nullable
    @Description("Number of bytes in each stripe.")
    private Long stripeSize;

    @Nullable
    @Description("Number of rows between index entries (must be >= 1,000)")
    private Long indexStride;

    @Nullable
    @Description("Whether to create inline indexes")
    private Boolean createIndex;

    public ORCDynamicPartitionedDatasetSinkConfig(String name, String schema, String fieldNames,
                                                  @Nullable String basePath, @Nullable String compressionCodec,
                                                  @Nullable String partitionWriteOption,
                                                  @Nullable Long compressionChunkSize, @Nullable Long stripeSize,
                                                  @Nullable Long indexStride,
                                                  @Nullable String createIndex) {
      super(name, schema, fieldNames, basePath, compressionCodec, partitionWriteOption);
      this.compressionCodec = compressionCodec;
      this.compressionChunkSize = compressionChunkSize;
      this.stripeSize = stripeSize;
      this.indexStride = indexStride;
      this.createIndex = (createIndex != null && createIndex.equals("True"));
    }

    @Override
    protected void validate(Schema inputSchema) {
      super.validate(inputSchema);

      // the following cconfigurations must be set if we are using a compression codec
      if (compressionCodec != null && !compressionCodec.equalsIgnoreCase("None")) {
        if (compressionChunkSize == null || stripeSize == null || indexStride == null || createIndex == null) {
          throw new IllegalArgumentException("compressionChunkSize, stripSize, indexStride, and createIndex must " +
                                               "all be set if using a different compressionCodec.");
        } else if (indexStride < 1000) {
          throw new IllegalArgumentException("indexStride must be >= 1000");
        }
      }
    }
  }

  /**
   * Dynamic partitioner that creates partitions based on a list of fields in each record.
   */
  public static final class OrcDynamicPartitioner extends FieldValueDynamicPartitioner<NullWritable, OrcStruct> {
    @Override
    public PartitionKey getPartitionKey(NullWritable key, OrcStruct value) {
      String[] fieldNames = stageFieldNames.get(value.getFieldValue(Constants.STAGE_FIELD_NAME).toString());
      PartitionKey.Builder keyBuilder = PartitionKey.builder();
      for (int i = 0; i < fieldNames.length; i++) {
        // discard leading and trailing white spaces in the partition name
        String columnValue = String.valueOf(value.getFieldValue(fieldNames[i])).trim();
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
