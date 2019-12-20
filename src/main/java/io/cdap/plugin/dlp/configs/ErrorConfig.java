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

/**
 * Class used for error targetting in the DLP Redaction plugin
 * This class defines the schema used by the frontend to determine the error message as well as which component/field
 * should be highlighted. This also adds support for displaying errors in nested components
 * ex. the maskingChar property within the Masking transform is invalid
 */
public class ErrorConfig {

  private String transform;
  private String fields;
  private String filters;
  private String transformPropertyId;
  private Boolean isNestedError;


  public ErrorConfig(DlpFieldTransformationConfig transformationConfig, String transformPropertyId,
                     Boolean isNestedError) {
    this.transform = transformationConfig.getTransform();
    this.fields = String.join(",", transformationConfig.getFields());
    this.filters = String.join(",", transformationConfig.getFilters());
    this.transformPropertyId = transformPropertyId;
    this.isNestedError = isNestedError;
  }

  public void setTransformPropertyId(String transformPropertyId) {
    this.isNestedError = false;
    this.transformPropertyId = transformPropertyId;
  }

  public void setNestedTransformPropertyId(String transformPropertyId) {
    this.isNestedError = true;
    this.transformPropertyId = transformPropertyId;
  }
}
