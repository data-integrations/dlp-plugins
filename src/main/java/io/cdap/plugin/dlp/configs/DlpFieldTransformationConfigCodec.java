/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.dlp.configs;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Custom codec for decoding DlpFieldTransformationConfig
 */
public class DlpFieldTransformationConfigCodec implements JsonDeserializer<DlpFieldTransformationConfig> {

  private final Map<String, Type> transformTypeDictionary = new HashMap<String, Type>() {{
    put("MASKING", MaskingTransformConfig.class);
  }};

  @Override
  public DlpFieldTransformationConfig deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
    throws JsonParseException {
    JsonObject jsonObj = json.getAsJsonObject();
    String transform = jsonObj.get("transform").getAsString();
    String fieldsString = context.deserialize(jsonObj.get("fields"), String.class);
    String[] fields = new String[]{};
    if (fieldsString.length() > 0) {
      fields = fieldsString.split(",");
    }

    String filtersString = context.deserialize(jsonObj.get("filters"), String.class);
    String[] filters = new String[]{};
    if (filtersString.length() > 0) {
      filters = filtersString.split(",");
    }

    if (!transformTypeDictionary.containsKey(transform)) {
      throw new JsonParseException(
        String.format("Transform %s does not have an associated transform config", transform));
    }

    DlpTransformConfig transformProperties = context
      .deserialize(jsonObj.get("transformProperties"), transformTypeDictionary.get(transform));

    return new DlpFieldTransformationConfig(transform, fields, filters, transformProperties);
  }

}
