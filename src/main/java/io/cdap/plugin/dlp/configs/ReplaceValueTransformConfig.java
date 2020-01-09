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

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import com.google.privacy.dlp.v2.ReplaceValueConfig;
import com.google.privacy.dlp.v2.Value;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 * Implementing the DlpTransformConfig interface for the DLP {@link ReplaceValueConfig} transform
 */
public class ReplaceValueTransformConfig implements DlpTransformConfig {

  private String newValue;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};
  private static final Gson gson = new Gson();

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    ReplaceValueConfig.Builder builder = ReplaceValueConfig.newBuilder();
    builder.setNewValue(Value.newBuilder().setStringValue(newValue).build());
    return PrimitiveTransformation.newBuilder().setReplaceConfig(builder).build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    if (Strings.isNullOrEmpty(newValue)) {
      errorConfig.setNestedTransformPropertyId("newValue");
      collector.addFailure("New Value is a required field for this transform.", "Please provide a value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    }
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
