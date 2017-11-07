package co.cask.hydrator.plugin.common;

import co.cask.cdap.api.data.schema.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility methods for schemas
 */
public class Schemas {

  private Schemas() {
    // no-op
  }

  /**
   * Add the specified field to the schema
   *
   * @param schema the schema to add the field to
   * @param field the field to add
   * @return schema with the field added
   */
  public static Schema addField(Schema schema, Schema.Field field) {
    List<Schema.Field> originalFields = schema.getFields();
    List<Schema.Field> fields = new ArrayList<>(originalFields.size() + 1);
    fields.addAll(originalFields);
    fields.add(field);
    return Schema.recordOf(schema.getRecordName(), fields);
  }
}
