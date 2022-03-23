/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.plugin.dlp;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.privacy.dlp.v2.LocationName;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.dlp.configs.CryptoDeterministicTransformationConfig;
import io.cdap.plugin.dlp.configs.CryptoHashTransformationConfig;
import io.cdap.plugin.dlp.configs.CryptoKeyHelper;
import io.cdap.plugin.dlp.configs.CryptoKeyHelper.KeyType;
import io.cdap.plugin.dlp.configs.DlpFieldTransformationConfig;
import io.cdap.plugin.dlp.configs.DlpFieldTransformationConfigCodec;
import io.cdap.plugin.dlp.configs.DlpTransformConfig;
import io.cdap.plugin.dlp.configs.ErrorConfig;
import io.cdap.plugin.gcp.common.GCPConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Common config used by the DLP Redact and Decrypt plugins
 */
public class DLPTransformPluginConfig extends GCPConfig {
  public static final String FIELDS_TO_TRANSFORM = "fieldsToTransform";
  public static final String CUSTOM_TEMPLATE_PATH_NAME = "customTemplatePath";
  public static final String TEMPLATE_ID_NAME = "templateId";
  public static final String CUSTOM_TEMPLATE_ENABLED_NAME = "customTemplateEnabled";
  public static final String LOCATION = "location";
  @Macro
  protected String fieldsToTransform;

  @Description("Enabling this option will allow you to define a custom DLP Inspection Template to use for matching "
    + "during the transform.")
  protected Boolean customTemplateEnabled;

  @Description("ID of the DLP Inspection template")
  @Macro
  @Nullable
  protected String templateId;

  @Description("Custom path of the DLP Inspection template")
  @Macro
  @Nullable
  protected String customTemplatePath;

  @Name(LOCATION)
  @Description("Resource location of DLP Service "
    + "(for more info: https://cloud.google.com/dlp/docs/specifying-location)")
  @Macro
  @Nullable
  protected String location;

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(DlpFieldTransformationConfig.class, new DlpFieldTransformationConfigCodec())
    .create();

  public List<DlpFieldTransformationConfig> parseTransformations() throws Exception {
    String[] values = GSON.fromJson(fieldsToTransform, String[].class);
    List<DlpFieldTransformationConfig> transformationConfigs = new ArrayList<>();
    for (String value : values) {
      transformationConfigs.add(GSON.fromJson(value, DlpFieldTransformationConfig.class));
    }
    return transformationConfigs;
  }

  /**
   * Get the set of fields that are being transformed or are required for transforms to work. This is used to limit
   * the payload size to DLP endpoints, the transform will only send the values of the required fields.
   *
   * @return Set of field names
   */
  public Set<String> getRequiredFields() throws Exception {
    return parseTransformations().stream()
      .map(DlpFieldTransformationConfig::getRequiredFields)
      .flatMap(Collection::stream)
      .collect(Collectors.toSet());
  }
  /**
   * Gets the custom template specified by user which can be either a templateId or a full custom template path
   * @return String of templateId or custom template path
   */
  public String getCustomTemplate() {
    return Strings.isNullOrEmpty(templateId) ? customTemplatePath :
      String.format("projects/%s/inspectTemplates/%s", getProject(), templateId);
  }

  public void validate(FailureCollector collector, Schema inputSchema) {
    if (customTemplateEnabled) {
      if (!containsMacro(TEMPLATE_ID_NAME) && !containsMacro(CUSTOM_TEMPLATE_PATH_NAME) &&
        Strings.isNullOrEmpty(templateId) && Strings.isNullOrEmpty(customTemplatePath)) {
        collector.addFailure("Custom template fields are not specified.",
                             "Must specify one of template id or template path")
          .withConfigProperty(TEMPLATE_ID_NAME).withConfigProperty(CUSTOM_TEMPLATE_PATH_NAME);
      }
      if (!containsMacro(TEMPLATE_ID_NAME) && !containsMacro(CUSTOM_TEMPLATE_PATH_NAME) &&
        !Strings.isNullOrEmpty(templateId) && !Strings.isNullOrEmpty(customTemplatePath)) {
        collector.addFailure("Both template id and template path are specified.",
                             "Must specify only one of template id or template path")
          .withConfigProperty(TEMPLATE_ID_NAME).withConfigProperty(CUSTOM_TEMPLATE_PATH_NAME);
      }
    }

    if (fieldsToTransform == null) {
      return;
    }
    try {
      List<DlpFieldTransformationConfig> transformationConfigs = parseTransformations();
      validateFieldConfigs(transformationConfigs, collector, inputSchema);
    } catch (Exception e) {
      collector.addFailure(String.format("Error while parsing transforms: %s", e.getMessage()), "")
        .withConfigProperty(FIELDS_TO_TRANSFORM);
    }
  }

  private void validateFieldConfigs(List<DlpFieldTransformationConfig> configs,
                                    FailureCollector collector, Schema inputSchema) {
    HashMap<String, String> transforms = new HashMap<>();
    Boolean firstTransformUsedCustomTemplate = null;
    Boolean anyTransformUsedCustomTemplate = false;
    for (DlpFieldTransformationConfig config : configs) {
      ErrorConfig errorConfig = config.getErrorConfig("");

      //Checking that custom template is defined if it is selected in one of the transforms
      List<String> filters = Arrays.asList(config.getFilters());
      if (!customTemplateEnabled && filters.contains("NONE")) {
        collector.addFailure(String.format("This transform depends on custom template that was not defined.",
                                           config.getTransform(), String.join(", ", config.getFields())),
                             "Enable the custom template option and provide the name of it.")
          .withConfigElement(FIELDS_TO_TRANSFORM, GSON.toJson(errorConfig));
      }
      //Validate the config for the transform
      config.validate(collector, inputSchema, FIELDS_TO_TRANSFORM);

      //Check that custom template and built-in types are not mixed
      anyTransformUsedCustomTemplate = anyTransformUsedCustomTemplate || filters.contains("NONE");
      if (firstTransformUsedCustomTemplate == null) {
        firstTransformUsedCustomTemplate = filters.contains("NONE");
      } else {
        if (filters.contains("NONE") != firstTransformUsedCustomTemplate) {
          errorConfig.setTransformPropertyId("filters");
          collector.addFailure("Cannot use custom templates and built-in filters in the same plugin instance.",
                               "All transforms must use custom templates or built-in filters, not a "
                                 + "combination of both.")
            .withConfigElement(FIELDS_TO_TRANSFORM, GSON.toJson(errorConfig));
        }
      }

      // Make sure the combination of field, transform and filter are unique
      for (String field : config.getFields()) {
        for (String filter : config.getFilterDisplayNames()) {
          String transformKey = String.format("%s:%s", field, filter);
          if (transforms.containsKey(transformKey)) {

            String errorMessage;
            if (transforms.get(transformKey).equalsIgnoreCase(config.getTransform())) {
              errorMessage = String.format(
                "Combination of transform, filter and field must be unique. Found multiple definitions for '%s' "
                  + "transform on '%s' with filter '%s'", config.getTransform(), field, filter);
            } else {
              errorMessage = String.format(
                "Only one transform can be defined per field and filter combination. Found conflicting transforms"
                  + " '%s' and '%s'",
                transforms.get(transformKey), config.getTransform());
            }
            errorConfig.setTransformPropertyId("");
            collector.addFailure(errorMessage, "")
              .withConfigElement(FIELDS_TO_TRANSFORM, GSON.toJson(errorConfig));
          } else {
            transforms.put(transformKey, config.getTransform());
          }
        }
      }

      // If the user has a custom template enabled but doesnt use it in any of the transforms
      if (!anyTransformUsedCustomTemplate && this.customTemplateEnabled) {
        collector.addFailure("Custom template is enabled but no transforms use a custom template.",
                             "Please define a transform that uses the custom template or disable the custom "
                                + "template.")
          .withConfigProperty("customTemplateEnabled");
      }

      //Checking if location of wrapped key is same as DLP resource location
      DlpTransformConfig transformProperties = config.getTransformProperties();
      if (transformProperties instanceof CryptoDeterministicTransformationConfig) {
        CryptoDeterministicTransformationConfig cryptoDeterministicTransformationConfig =
          (CryptoDeterministicTransformationConfig) transformProperties;
        if (cryptoDeterministicTransformationConfig.getKeyType() == KeyType.KMS_WRAPPED) {
          String cryptoKeyName = cryptoDeterministicTransformationConfig.getCryptoKeyName();
          CryptoKeyHelper.
            validateCryptoKeyNameLocation(location, cryptoKeyName, collector, FIELDS_TO_TRANSFORM, LOCATION);
        }
      } else if (transformProperties instanceof CryptoHashTransformationConfig) {
        CryptoHashTransformationConfig cryptoHashTransformationConfig =
          (CryptoHashTransformationConfig) transformProperties;
        if (cryptoHashTransformationConfig.getKeyType() == KeyType.KMS_WRAPPED) {
          String cryptoKeyName = cryptoHashTransformationConfig.getCryptoKeyName();
          CryptoKeyHelper.
            validateCryptoKeyNameLocation(location, cryptoKeyName, collector, FIELDS_TO_TRANSFORM, LOCATION);
        }
      }
    }
  }

  @Nullable
  public String getLocation() {
    return location;
  }

  public LocationName getLocationNameUtil(String project, @Nullable String location) {
    if (Strings.isNullOrEmpty(location)) {
      location = "global";
    }
    return LocationName.of(project, location);
  }

  public LocationName getLocationName() {
    return getLocationNameUtil(getProject(), getLocation());
  }


}
