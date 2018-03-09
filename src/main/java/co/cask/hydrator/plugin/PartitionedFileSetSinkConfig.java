/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.common.HiveSchemaConverter;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Abstract config for TimePartitionedFileSetSink
 */
public class PartitionedFileSetSinkConfig extends PluginConfig {
  public static final String FIELD_NAMES_PROPERTY_KEY = "fieldNames";

  @Description("Name of the Partitioned FileSet Dataset to which the records " +
    "are written to. If it doesn't exist, it will be created.")
  @Macro
  protected String name;

  @Description("The base path for the Partitioned FileSet. Defaults to the name of the dataset.")
  @Nullable
  @Macro
  protected String basePath;

  @Nullable
  @Description("Used to specify the compression codec to be used for the final dataset.")
  protected String compressionCodec;

  @Description("The schema of the record being written to the Sink as a JSON Object.")
  @Macro
  protected String schema;

  @Description("The fields to be used for the partitions as comma separated values.")
  @Macro
  protected String fieldNames;

  @Nullable
  @Description("Allow appending to existing partitions, by default this capability is disabled.")
  protected String appendToPartition;

  public PartitionedFileSetSinkConfig(String name, String schema, String fieldNames,
                                      @Nullable String basePath, @Nullable String compressionCodec,
                                      @Nullable String appendToPartition) {
    this.name = name;
    this.basePath = basePath;
    this.compressionCodec = compressionCodec;
    this.schema = schema;
    this.fieldNames = fieldNames;
    this.appendToPartition = appendToPartition;
  }

  public String getNonNullBasePath() {
    return this.basePath == null ? this.name : this.basePath;
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
    }
  }

  protected Map.Entry<Schema, String> getOutputSchema() {
    // parse to make sure it's valid
    new org.apache.avro.Schema.Parser().parse(this.schema);
    Schema parsedSchema;
    try {
      parsedSchema = Schema.parseJson(this.schema);
      HiveSchemaConverter.toHiveSchema(parsedSchema);
    } catch (UnsupportedTypeException | IOException e) {
      throw new IllegalArgumentException("Error: Schema is not valid ", e);
    }

    // add the fields that aren't part of the partitioning fields to the output schema
    List<Schema.Field> fields = new ArrayList<>();
    List<String> toRemove = Arrays.asList(this.fieldNames.split(","));
    for (Schema.Field field : parsedSchema.getFields()) {
      if (!toRemove.contains(field.getName())) {
        fields.add(field);
      }
    }
    String outputHiveSchema;
    Schema outputSchema;
    try {
      outputSchema = Schema.recordOf("output", fields);
      outputHiveSchema = HiveSchemaConverter.toHiveSchema(outputSchema);
    } catch (UnsupportedTypeException e) {
      throw new IllegalArgumentException("Error: Schema is not valid ", e);
    }

    return new AbstractMap.SimpleEntry<>(outputSchema, outputHiveSchema);
  }

  protected Partitioning getPartitioningWithoutMacro(@Nullable Schema inputSchema) {
    Partitioning.Builder partitionBuilder = Partitioning.builder();
    String[] partitionFields = this.fieldNames.split(",");
    for (int i = 0; i < partitionFields.length; i++) {
      if (inputSchema != null) {
        if (inputSchema.getField(partitionFields[i]) == null) {
          // throw exception if the field used to partition is not present in the input schema
          throw new IllegalArgumentException(String.format("Field %s is not present in the input schema.",
                                             partitionFields[i]));
        } else if (inputSchema.getField(partitionFields[i]).getSchema().isNullable()) {
          // throw exception if input field is nullable
          throw new IllegalArgumentException(String.format("Input field %s has to be non-nullable.",
                                             partitionFields[i]));
        }
      }
      partitionBuilder.addStringField(partitionFields[i]);
    }
    return partitionBuilder.build();
  }

  /**
   * Validate plugin configuration at deployment stage.
   */
  protected void validate(@Nullable Schema inputSchema) {
    if (inputSchema == null) {
      return;
    }
    // No need to validate partition fields if it is macro enabled. No need to validate output schema if
    // partition fields or output schema is macro enabled.
    if (!this.containsMacro("fieldNames")) {
      getPartitioningWithoutMacro(inputSchema);
      if (!this.containsMacro("schema")) {
        getOutputSchema();
      }
    }
  }
}
