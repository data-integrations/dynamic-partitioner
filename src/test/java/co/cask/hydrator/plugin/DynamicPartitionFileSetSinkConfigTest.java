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

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.etl.mock.common.MockPipelineConfigurer;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test configuration of dynamic partitioned fileset sink plugins.
 */
public class DynamicPartitionFileSetSinkConfigTest {

  private final String recordName = "sales";
  private final String avroSinkName = recordName + "Avro";
  private final String parquetSinkName = recordName + "Parquet";
  private final String orcSinkName = recordName + "ORC";

  @Test
  public void testSinglePartitionPipelineConfiguration() {
    Schema schema = Schema.recordOf(
      recordName,
      Schema.Field.of("product", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // use price as one layer partition field
    String fieldnames = "price";

    // configure the pipelines
    configureAvroSinkfromConfigs(avroSinkName, schema, schema, fieldnames, null, null, null);
    configureParquetSinkfromConfigs(parquetSinkName, schema, schema, fieldnames, null, null, null);
  }

  @Test
  public void testMultiPartitionPipelineConfiguration() {
    Schema schema = Schema.recordOf(
      recordName,
      Schema.Field.of("product", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // use multiple partition fields
    String fieldnames = "price,buyer";

    // configure the pipelines
    configureAvroSinkfromConfigs(avroSinkName, schema, schema, fieldnames, null, null, null);
    configureParquetSinkfromConfigs(parquetSinkName, schema, schema, fieldnames, null, null, null);
  }

  @Test
  public void testNonExistentFieldPipelineConfiguration() {
    Schema inputschema = Schema.recordOf(
      recordName,
      Schema.Field.of("product", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // output schema shouldn't matter since configurePipeline method looks at input schema
    Schema outputschema = Schema.recordOf(
      recordName,
      Schema.Field.of("prooduct", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // use price as one layer partition field
    String fieldnames = "prooduct";

    // configure the pipelines
    try {
      configureAvroSinkfromConfigs(avroSinkName, inputschema, outputschema, fieldnames, null, null, null);
      Assert.fail("Avro sink did not throw exception");
    } catch (RuntimeException e) {
    }

    try {
      configureParquetSinkfromConfigs(avroSinkName, inputschema, outputschema, fieldnames, null, null, null);
      Assert.fail("Parquet sink did not throw exception");
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testNullableInputFieldPipelineConfiguration() {
    Schema inputschema = Schema.recordOf(
      recordName,
      Schema.Field.of("product", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // output schema shouldn't matter since configurePipeline method looks at input schema
    Schema outputschema = Schema.recordOf(
      recordName,
      Schema.Field.of("prooduct", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // use price as one layer partition field
    String fieldnames = "buyer";

    // configure the pipelines
    try {
      configureAvroSinkfromConfigs(avroSinkName, inputschema, outputschema, fieldnames, null, null, null);
      Assert.fail("Avro sink did not throw exception");
    } catch (RuntimeException e) {
    }
  }

  private void configureAvroSinkfromConfigs(String sinkName, Schema inputschema, Schema outputschema,
                                            String fieldnames, String basePath, String compressionCodec,
                                            String partitionWriteOption) {
    // test avro sink
    PartitionedFileSetSinkConfig avroDynamicPartitionedDatasetSinkConfig =
      new PartitionedFileSetSinkConfig(sinkName, outputschema.toString(), fieldnames, basePath, compressionCodec,
                                       partitionWriteOption);
    AvroDynamicPartitionedDatasetSink avroDynamicPartitionedDatasetSink =
      new AvroDynamicPartitionedDatasetSink(avroDynamicPartitionedDatasetSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputschema);
    avroDynamicPartitionedDatasetSink.configurePipeline(mockPipelineConfigurer);
  }

  private void configureParquetSinkfromConfigs(String sinkName, Schema inputschema, Schema outputschema,
                                               String fieldnames, String basePath, String compressionCodec,
                                               String partitionWriteOption) {
    // test parquet sink
    PartitionedFileSetSinkConfig parquetDynamicPartitionedDatasetSinkConfig =
      new PartitionedFileSetSinkConfig(sinkName, outputschema.toString(), fieldnames, basePath, compressionCodec,
                                       partitionWriteOption);
    ParquetDynamicPartitionedDatasetSink parquetDynamicPartitionedDatasetSink =
      new ParquetDynamicPartitionedDatasetSink(parquetDynamicPartitionedDatasetSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputschema);
    parquetDynamicPartitionedDatasetSink.configurePipeline(mockPipelineConfigurer);
  }
}
