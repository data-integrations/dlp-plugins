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

import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.List;

/**
 * Interface for all DLP Transformations supported by the Redaction Plugin
 */
public interface DlpTransformConfig {

  /**
   * Construct DLP PrimitiveTransform from this object's properties
   *
   * @return PrimitiveTransform object with all required fields filled out
   */
  PrimitiveTransformation toPrimitiveTransform();

  /**
   * Validates that the current set of properties in this object can form a valid FieldTransform object
   *
   * @param collector   FailureCollector that will be used to record all errors
   * @param widgetName  Name of the widget/field in the config that will be highlighted if an error is found
   * @param errorConfig Template ErrorConfig object to be used for correctly targetting errors in the FailureCollector
   */
  void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig);

  /**
   * List of supported Schema.Type for this plugin, this will be checked against the input schema during the
   * validation stage
   *
   * @return List of supported types
   */
  List<Schema.Type> getSupportedTypes();

}
