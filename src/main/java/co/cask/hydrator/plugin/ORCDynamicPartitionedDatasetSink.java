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
import co.cask.hydrator.plugin.common.StructuredToOrcTransformer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.orc.mapred.OrcStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link BatchSink} to write ORC records to a {@link PartitionedFileSet}.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("ORCDynamicPartitionedDataset")
@Description("Sink for a PartitionedFileSet that writes data in ORC format and uses a dynamic partition key.")
public class ORCDynamicPartitionedDatasetSink extends
  PartitionedFileSetSink<NullWritable, OrcStruct> {
  public static final String NAME = "ORCDynamicPartitionedDataset";

  private static final Logger LOG = LoggerFactory.getLogger(ORCDynamicPartitionedDatasetSink.class);

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
    Map<String, String> sinkArgs = getAdditionalPFSArguments();
    PartitionedFileSetArguments.setDynamicPartitioner
      (sinkArgs, ORCDynamicPartitionedDatasetSink.FieldValueDynamicPartitioner.class);
    context.addOutput(Output.ofDataset(config.name, sinkArgs));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToOrcTransformer();
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<NullWritable, OrcStruct>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), recordTransformer.transform(input, input.getSchema())));
  }

  @Override
  protected void addPartitionedFileSetProperties(PartitionedFileSetProperties.Builder properties) {
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
  }

  @Override
  protected DatasetProperties getDatasetProperties(Partitioning partitioning,
                                                   Map.Entry<Schema, String> outputSchemaPair) {
    PartitionedFileSetProperties.Builder properties = PartitionedFileSetProperties.builder();
    addPartitionedFileSetProperties(properties);
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
                                         @Nullable Long compressionChunkSize, @Nullable Long stripeSize,
                                         @Nullable Long indexStride,
                                         @Nullable String createIndex) {
      super(name, schema, fieldNames, basePath, compressionCodec);
      this.compressionCodec = compressionCodec;
      this.compressionChunkSize = compressionChunkSize;
      this.stripeSize = stripeSize;
      this.indexStride = indexStride;
      this.createIndex = (createIndex != null && createIndex.equals("True"));
    }

    @Override
    protected void validate (Schema inputSchema) {
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
  public static final class FieldValueDynamicPartitioner
    extends DynamicPartitioner<NullWritable, OrcStruct> {
    private String[] fieldNames;

    @Override
    public void initialize(MapReduceTaskContext mapReduceTaskContext) {
      // Need a better way to do this. [CDAP-7058]
      String rawFieldNames = mapReduceTaskContext
        .getPluginProperties(ORCDynamicPartitionedDatasetSink.NAME)
        .getProperties().get(ORCDynamicPartitionedDatasetSinkConfig.FIELD_NAMES_PROPERTY_KEY);

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
    public PartitionKey getPartitionKey(NullWritable key, OrcStruct value) {
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
