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
import org.junit.Test;
import org.junit.Assert;

import javax.annotation.Nullable;


public class DynamicPartitionFileSetSinkTest {

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
    configureAvroSinkfromConfigs(avroSinkName, schema, schema, fieldnames, null, null);
    configureParquetSinkfromConfigs(parquetSinkName, schema, schema, fieldnames, null, null);
    configureORCSinkfromConfigs(orcSinkName, schema, schema, fieldnames, null, null, null, null, null, null);
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
    configureAvroSinkfromConfigs(avroSinkName, schema, schema, fieldnames, null, null);
    configureParquetSinkfromConfigs(parquetSinkName, schema, schema, fieldnames, null, null);
    configureORCSinkfromConfigs(orcSinkName, schema, schema, fieldnames, null, null, null, null, null, null);
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
      configureAvroSinkfromConfigs(avroSinkName, inputschema, outputschema, fieldnames, null, null);
      Assert.fail("Avro sink did not throw exception");
    } catch (RuntimeException e) {
    }

    try {
      configureParquetSinkfromConfigs(avroSinkName, inputschema, outputschema, fieldnames, null, null);
      Assert.fail("Parquet sink did not throw exception");
    } catch (RuntimeException e) {
    }

    try {
      configureORCSinkfromConfigs(orcSinkName, inputschema, outputschema, fieldnames,
                                  null, null, null, null, null, null);
      Assert.fail("ORC sink did not throw exception");
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
      configureAvroSinkfromConfigs(avroSinkName, inputschema, outputschema, fieldnames, null, null);
      Assert.fail("Avro sink did not throw exception");
    } catch (RuntimeException e) {
    }

    try {
      configureORCSinkfromConfigs(orcSinkName, inputschema, outputschema, fieldnames,
                                  null, null, null, null, null, null);
      Assert.fail("ORC sink did not throw exception");
    } catch (RuntimeException e) {
    }
  }

  @Test
  public void testORCSinkPipelineConfiguration() {
    Schema schema = Schema.recordOf(
      recordName,
      Schema.Field.of("product", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("buyer", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("price", Schema.of(Schema.Type.DOUBLE))
    );

    // use multiple partition fields
    String fieldnames = "price,buyer";

    // configure the pipelines
    configureORCSinkfromConfigs(orcSinkName, schema, schema, fieldnames, null, "ZLIB",
                                new Long("1024"), new Long("1024"), new Long("1000"), "True");

    // one field is missing when codec is set
    try {
      configureORCSinkfromConfigs(orcSinkName, schema, schema, fieldnames, null, "ZLIB",
                                  new Long("1024"), null, new Long("1024"), "True");
      Assert.fail("ORC sink did not throw exception");
    } catch (RuntimeException e) {
    }

    // index stride smaller than 1000
    try {
      configureORCSinkfromConfigs(orcSinkName, schema, schema, fieldnames, null, "ZLIB",
                                  new Long("1024"), new Long("1024"), new Long("999"), "True");
      Assert.fail("ORC sink did not throw exception");
    } catch (RuntimeException e) {
    }
  }

  private void configureAvroSinkfromConfigs(String sinkName, Schema inputschema, Schema outputschema,
                                             String fieldnames, String basePath, String compressionCodec) {
    // test avro sink
    AvroDynamicPartitionedDatasetSink.AvroDynamicPartitionedDatasetSinkConfig AvroDynamicPartitionedDatasetSinkConfig =
      new AvroDynamicPartitionedDatasetSink.AvroDynamicPartitionedDatasetSinkConfig(sinkName, outputschema.toString(), fieldnames,
                                                                         basePath, compressionCodec);
    AvroDynamicPartitionedDatasetSink AvroDynamicPartitionedDatasetSink =
      new AvroDynamicPartitionedDatasetSink(AvroDynamicPartitionedDatasetSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputschema);
    AvroDynamicPartitionedDatasetSink.configurePipeline(mockPipelineConfigurer);
  }

  private void configureParquetSinkfromConfigs(String sinkName, Schema inputschema, Schema outputschema,
                                                String fieldnames, String basePath, String compressionCodec) {
    // test parquet sink
    ParquetDynamicPartitionedDatasetSink.ParquetDynamicPartitionedDatasetSinkConfig ParquetDynamicPartitionedDatasetSinkConfig =
      new ParquetDynamicPartitionedDatasetSink.ParquetDynamicPartitionedDatasetSinkConfig(sinkName, outputschema.toString(),
                                                                               fieldnames, basePath, compressionCodec);
    ParquetDynamicPartitionedDatasetSink ParquetDynamicPartitionedDatasetSink =
      new ParquetDynamicPartitionedDatasetSink(ParquetDynamicPartitionedDatasetSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputschema);
    ParquetDynamicPartitionedDatasetSink.configurePipeline(mockPipelineConfigurer);
  }

  private void configureORCSinkfromConfigs(String sinkName, Schema inputschema, Schema outputschema,
                                           String fieldnames, String basePath, String compressionCodec,
                                           Long compressionChunkSize, Long stripeSize,
                                           Long indexStride,
                                           String createIndex) {
    // test parquet sink
    ORCDynamicPartitionedDatasetSink.ORCDynamicPartitionedDatasetSinkConfig ORCDynamicPartitionedDatasetSinkConfig =
      new ORCDynamicPartitionedDatasetSink.ORCDynamicPartitionedDatasetSinkConfig(sinkName, outputschema.toString(),
                                                                       fieldnames, basePath, compressionCodec,
                                                                       compressionChunkSize, stripeSize, indexStride,
                                                                       createIndex);
    ORCDynamicPartitionedDatasetSink ORCDynamicPartitionedDatasetSink =
      new ORCDynamicPartitionedDatasetSink(ORCDynamicPartitionedDatasetSinkConfig);

    MockPipelineConfigurer mockPipelineConfigurer = new MockPipelineConfigurer(inputschema);
    ORCDynamicPartitionedDatasetSink.configurePipeline(mockPipelineConfigurer);
  }
}