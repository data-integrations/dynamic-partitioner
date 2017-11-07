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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.Engine;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcMapreduceRecordWriter;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for running dynamic partitioned fileset sink plugins.
 */
public class DynamicPartitionedFilesetSinkTest extends HydratorTestBase {
  private static final ArtifactId APP_ARTIFACT_ID = NamespaceId.DEFAULT.artifact("app", "1.0.0");
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("app", "1.0.0");
  private static final Schema SCHEMA = Schema.recordOf(
    "purchase",
    Schema.Field.of("id", Schema.of(Schema.Type.LONG)),
    Schema.Field.of("first_name", Schema.of(Schema.Type.STRING)),
    Schema.Field.of("purchase_date", Schema.of(Schema.Type.STRING)));
  private static int startCount = 0;

  @BeforeClass
  public static void setupTest() throws Exception {
    if (startCount++ > 0) {
      return;
    }
    setupBatchArtifacts(APP_ARTIFACT_ID, DataPipelineApp.class);
    // add some test plugins
    addPluginArtifact(NamespaceId.DEFAULT.artifact("dynamic-partitioned-dataset-sink-plugins", "1.0.0"),
                      APP_ARTIFACT_ID,
                      AvroDynamicPartitionedDatasetSink.class, AvroKeyOutputFormat.class,
                      AvroKey.class, AvroSerialization.class, ReflectData.class,
                      ORCDynamicPartitionedDatasetSink.class,
                      OrcStruct.class, OrcMapreduceRecordWriter.class, TimestampColumnVector.class,
                      ParquetDynamicPartitionedDatasetSink.class, AvroParquetOutputFormat.class);
  }

  @Test
  public void testAvroPartitionByPurchaseDate() throws Exception {
    testDynamicPartition(AvroDynamicPartitionedDatasetSink.NAME);
  }

  @Test
  public void testParquetPartitionByPurchaseDate() throws Exception {
    testDynamicPartition(ParquetDynamicPartitionedDatasetSink.NAME);
  }

  @Test
  public void testOrcPartitionByPurchaseDate() throws Exception {
    testDynamicPartition(ORCDynamicPartitionedDatasetSink.NAME);
  }

  private void testDynamicPartition(String sinkPluginName) throws Exception {
    String sourceName = "source-" + sinkPluginName;
    String sinkName = "sink-" + sinkPluginName;
    Map<String, String> properties = ImmutableMap.of("name", sinkName,
                                                     "schema", SCHEMA.toString(),
                                                     "fieldNames", "purchase_date",
                                                     "appendToPartition", "CREATE_OR_APPEND");
    ETLStage purchaseSink =
      new ETLStage(sinkPluginName, new ETLPlugin(sinkPluginName, BatchSink.PLUGIN_TYPE, properties, null));
    ETLBatchConfig etlConfig = co.cask.cdap.etl.proto.v2.ETLBatchConfig.builder("* * * * *")
      .addStage(new ETLStage("source", MockSource.getPlugin(sourceName, SCHEMA)))
      .addStage(purchaseSink)
      .addConnection("source", sinkPluginName)
      .setEngine(Engine.MAPREDUCE)
      .build();

    AppRequest<ETLBatchConfig> appRequest = new AppRequest<>(APP_ARTIFACT, etlConfig);
    ApplicationId appId = NamespaceId.DEFAULT.app("Test_" + sinkPluginName);
    ApplicationManager appManager = deployApplication(appId, appRequest);

    StructuredRecord record1 = StructuredRecord.builder(SCHEMA).set("id", 1L)
      .set("first_name", "Douglas").set("purchase_date", "2009-01-02").build();
    StructuredRecord record2 = StructuredRecord.builder(SCHEMA).set("id", 2L)
      .set("first_name", "David").set("purchase_date", "2009-01-01").build();
    StructuredRecord record3 = StructuredRecord.builder(SCHEMA).set("id", 3L)
      .set("first_name", "Hugh").set("purchase_date", "2009-01-01").build();
    StructuredRecord record4 = StructuredRecord.builder(SCHEMA).set("id", 4L)
      .set("first_name", "Walter").set("purchase_date", "2009-01-03").build();
    StructuredRecord record5 = StructuredRecord.builder(SCHEMA).set("id", 5L)
      .set("first_name", "Frank").set("purchase_date", "2009-01-03").build();
    StructuredRecord record6 = StructuredRecord.builder(SCHEMA).set("id", 6L)
      .set("first_name", "Serena").set("purchase_date", "2009-01-01").build();
    DataSetManager<Table> sourceTable = getDataset(sourceName);
    MockSource.writeInput(sourceTable, ImmutableList.of(record1, record2, record3, record4, record5, record6));

    WorkflowManager manager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    manager.start();
    manager.waitForRun(ProgramRunStatus.COMPLETED, 3, TimeUnit.MINUTES);

    DataSetManager<PartitionedFileSet> pfsManager = getDataset(sinkName);
    pfsManager.flush();
    PartitionedFileSet partionedRecords = pfsManager.get();
    Set<PartitionDetail> partitions = partionedRecords.getPartitions(null);
    Assert.assertEquals(3, partitions.size());
    Assert.assertNotNull(partionedRecords.getPartition(new PartitionKey.Builder()
                                                         .addField("purchase_date", "2009-01-01").build()));
    Assert.assertNotNull(partionedRecords.getPartition(new PartitionKey.Builder()
                                                         .addField("purchase_date", "2009-01-02").build()));
    Assert.assertNotNull(partionedRecords.getPartition(new PartitionKey.Builder()
                                                         .addField("purchase_date", "2009-01-03").build()));
  }
}
