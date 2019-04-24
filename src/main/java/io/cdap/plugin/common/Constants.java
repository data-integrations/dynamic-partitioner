package io.cdap.plugin.common;

import io.cdap.cdap.api.data.schema.Schema;

/**
 * Constants.
 */
public class Constants {
  public static final String STAGE_FIELD_NAME = "_CDAPStageName";
  public static final Schema.Field STAGE_FIELD = Schema.Field.of(STAGE_FIELD_NAME, Schema.of(Schema.Type.STRING));
}
