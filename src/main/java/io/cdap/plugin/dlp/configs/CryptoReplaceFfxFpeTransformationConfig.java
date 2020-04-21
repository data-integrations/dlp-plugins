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

package io.cdap.plugin.dlp.configs;

import com.google.common.base.Strings;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.CryptoReplaceFfxFpeConfig;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Implementing the DlpTransformConfig interface for the DLP {@link CryptoHashConfig}
 */
public class CryptoReplaceFfxFpeTransformationConfig implements DlpTransformConfig {

  private static final String INFO_TYPE_NAME_REGEX = "[a-zA-Z0-9_]{1,64}";
  private String key;
  private String name;
  private String wrappedKey;
  private String cryptoKeyName;
  private CryptoKeyHelper.KeyType keyType;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};
  private String surrogateInfoTypeName;
  private String alphabet;
  private String customAlphabet;
  private String context;
  private static final Gson gson = new Gson();

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    CryptoKey cryptoKey = CryptoKeyHelper.createKey(keyType, name, key, cryptoKeyName, wrappedKey);
    InfoType surrogateType = InfoType.newBuilder().setName(surrogateInfoTypeName).build();

    CryptoReplaceFfxFpeConfig.Builder configBuilder = CryptoReplaceFfxFpeConfig.newBuilder();

    configBuilder.setCryptoKey(cryptoKey).setSurrogateInfoType(surrogateType);

    if (!Strings.isNullOrEmpty(context)) {
      configBuilder.setContext(FieldId.newBuilder().setName(context).build());
    }

    if (alphabet.equals("CUSTOM")) {
      configBuilder.setCustomAlphabet(customAlphabet);
    } else {
      configBuilder.setCommonAlphabet(CryptoReplaceFfxFpeConfig.FfxCommonNativeAlphabet.valueOf(alphabet));
    }

    return PrimitiveTransformation.newBuilder().setCryptoReplaceFfxFpeConfig(configBuilder).build();
  }


  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    CryptoKeyHelper.validateKey(collector, widgetName, errorConfig, keyType, name, key, cryptoKeyName, wrappedKey);

    if (Strings.isNullOrEmpty(surrogateInfoTypeName)) {
      errorConfig.setNestedTransformPropertyId("surrogateInfoTypeName");
      collector.addFailure("Surrogate Type Name is a required field.", "Please provide a valid value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else {
      Pattern pattern = Pattern.compile(INFO_TYPE_NAME_REGEX);
      boolean isValidSurrogateName = pattern.matcher(surrogateInfoTypeName).matches();
      if (!isValidSurrogateName) {
        errorConfig.setNestedTransformPropertyId("surrogateInfoTypeName");
        collector.addFailure(String.format("Value of '%s' is not valid for Surrogate Type Name", surrogateInfoTypeName),
                             "Name can only contain upper/lowercase letters, numbers or underscores and" +
                               "it must be between 1 and 64 characters long.")
          .withConfigElement(widgetName, gson.toJson(errorConfig));
      }
    }

    if (Strings.isNullOrEmpty(alphabet)) {
      errorConfig.setNestedTransformPropertyId("alphabet");
      collector.addFailure("Alphabet Type is a required field.", "Please provide a valid value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else if (alphabet.equals("CUSTOM") && Strings.isNullOrEmpty(customAlphabet)) {
      errorConfig.setNestedTransformPropertyId("customAlphabet");
      collector.addFailure("Custom Alphabet Type is a required field if Alphabet Type is set to 'Custom'.",
                           "Please provide a valid value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    } else if (alphabet.equals("CUSTOM") && (customAlphabet.length() < 2 || customAlphabet.length() > 95)) {
      errorConfig.setNestedTransformPropertyId("customAlphabet");
      collector.addFailure(String.format("Custom Alphabet Type must be between 2 and 95 characters long, current " +
                                           "length is %d.", customAlphabet.length()), "")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
    }
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }

  @Override
  public Set<String> getRequiredFields() {
    return new HashSet<>(Collections.singleton(context));
  }
}
