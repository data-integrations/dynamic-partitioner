package co.cask.hydrator.plugin;

import co.cask.cdap.api.dataset.lib.DynamicPartitioner;
import co.cask.cdap.api.mapreduce.MapReduceTaskContext;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Dynamic partitioner that creates partitions based on a list of fields in each record.
 *
 * @param <K> type of key
 * @param <V> type of value
 */
public abstract class FieldValueDynamicPartitioner<K, V> extends DynamicPartitioner<K, V> {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  protected Map<String, String[]> stageFieldNames;

  @Override
  public void initialize(MapReduceTaskContext mapReduceTaskContext) {
    Map<String, String> stageFieldNames =
      GSON.fromJson(mapReduceTaskContext.getWorkflowToken().get("fieldnames").toString(), MAP_TYPE);

    this.stageFieldNames = new HashMap<>(stageFieldNames.size());
    for (Map.Entry<String, String> entry : stageFieldNames.entrySet()) {
      this.stageFieldNames.put(entry.getKey(), entry.getValue().split(","));
    }
  }
}
