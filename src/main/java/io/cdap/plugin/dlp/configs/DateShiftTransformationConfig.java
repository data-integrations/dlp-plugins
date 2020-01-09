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

import com.google.gson.Gson;
import com.google.privacy.dlp.v2.DateShiftConfig;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 * Implementing the DlpTransformConfig interface for the DLP {@link DateShiftConfig}
 */
public class DateShiftTransformationConfig implements DlpTransformConfig {

  private Integer upperBoundDays;
  private Integer lowerBoundDays;

  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.INT, Schema.Type.LONG};
  private static final Gson gson = new Gson();

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {

    DateShiftConfig dateShiftConfig = DateShiftConfig.newBuilder()
      .setLowerBoundDays(lowerBoundDays)
      .setUpperBoundDays(upperBoundDays)
      .build();

    return PrimitiveTransformation.newBuilder().setDateShiftConfig(dateShiftConfig).build();
  }

  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    // CryptoKeyHelper.validateKey(collector, widgetName, errorConfig, keyType, name, key, cryptoKeyName, wrappedKey);

    if (upperBoundDays == null) {
      errorConfig.setNestedTransformPropertyId("upperBoundDays");
      collector.addFailure("Upper Bound is a required field for this transform.", "Please provide a value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else {
      if (Math.abs(upperBoundDays) > 365250) {
        errorConfig.setNestedTransformPropertyId("upperBoundDays");
        collector.addFailure("Upper Bound cannot be more than 10 years (365250 days) in either direction.",
                             "Please provide that is within this range.")
          .withConfigElement(widgetName, gson.toJson(errorConfig));
      }
    }

    if (lowerBoundDays == null) {
      errorConfig.setNestedTransformPropertyId("lowerBoundDays");
      collector.addFailure("Lower Bound is a required field for this transform.", "Please provide a value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else {
      if (Math.abs(lowerBoundDays) > 365250) {
        errorConfig.setNestedTransformPropertyId("upperBoundDays");
        collector.addFailure("Lower Bound cannot be more than 10 years (365250 days) in either direction.",
                             "Please provide that is within this range.")
          .withConfigElement(widgetName, gson.toJson(errorConfig));
      }
    }

    if (lowerBoundDays != null && upperBoundDays != null && lowerBoundDays > upperBoundDays) {
      errorConfig.setNestedTransformPropertyId("lowerBoundDays");
      collector.addFailure("Lower Bound cannot be greater than Upper Bound.", "")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    }

  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
