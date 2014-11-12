package org.nebulostore.utils;

import java.util.Collection;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

/**
 * @author lukaszsiczek
 */
public final class JSONFactory {

  private JSONFactory() {
  }

  public static JsonElement recursiveConvertFromMap(Map<?, ?> map) {
    return (new Gson()).toJsonTree(map);
  }

  public static JsonObject convertFromMap(Map<?, ?> map) {
    JsonObject jsonObject = new JsonObject();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      jsonObject.addProperty(entry.getKey().toString(), entry.getValue().toString());
    }
    return jsonObject;
  }

  public static JsonElement convertFromCollection(Collection<?> collection) {
    JsonArray jsonArray = new JsonArray();
    for (Object element : collection) {
      jsonArray.add(new JsonPrimitive(element.toString()));
    }
    return jsonArray;
  }
}
