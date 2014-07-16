package org.nebulostore.utils;

import java.util.List;
import java.util.Map;

import com.google.gson.JsonObject;

/**
 * @author lukaszsiczek
 */
public final class JSONFactory {

  private JSONFactory() {
  }

  public static JsonObject convertFromMap(Map<?, ?> map) {
    JsonObject jsonObject = new JsonObject();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      jsonObject.addProperty(entry.getKey().toString(), entry.getValue().toString());
    }
    return jsonObject;
  }

  public static JsonObject convertFromList(List<?> list) {
    int i = 0;
    JsonObject jsonObject = new JsonObject();
    for (Object element : list) {
      jsonObject.addProperty(Integer.toString(i), element.toString());
      ++i;
    }
    return jsonObject;
  }
}
