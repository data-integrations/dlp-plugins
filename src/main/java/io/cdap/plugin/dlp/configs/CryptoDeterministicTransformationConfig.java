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

import com.google.api.client.util.Strings;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.CryptoDeterministicConfig;
import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Implementing the DlpTransformConfig interface for the DLP {@link CryptoDeterministicConfig}
 */
public class CryptoDeterministicTransformationConfig implements DlpTransformConfig {

  private static final String INFO_TYPE_NAME_REGEX = "[a-zA-Z0-9_]{1,64}";
  private String key;
  private String name;
  private String wrappedKey;
  private String cryptoKeyName;
  private CryptoKeyHelper.KeyType keyType;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};
  private String surrogateInfoTypeName;
  private String context;
  private static final Gson gson = new Gson();

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {
    CryptoKey cryptoKey = CryptoKeyHelper.createKey(keyType, name, key, cryptoKeyName, wrappedKey);
    InfoType surrogateType = InfoType.newBuilder().setName(surrogateInfoTypeName).build();

    CryptoDeterministicConfig.Builder configBuilder = CryptoDeterministicConfig.newBuilder();
    configBuilder.setCryptoKey(cryptoKey).setSurrogateInfoType(surrogateType);

    if (!Strings.isNullOrEmpty(context)) {
      configBuilder.setContext(FieldId.newBuilder().setName(context).build());
    }

    return PrimitiveTransformation.newBuilder().setCryptoDeterministicConfig(configBuilder).build();
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
        collector.addFailure(String
                               .format("Value of '%s' is not valid for Surrogate Type Name", surrogateInfoTypeName),
                             "Name can only contain upper/lowercase letters, numbers or underscores and" +
                               "it must be between 1 and 64 characters long.")
          .withConfigElement(widgetName, gson.toJson(errorConfig));
      }
    }
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }

  public CryptoKeyHelper.KeyType getKeyType() {
    return keyType;
  }

  public String getCryptoKeyName() {
    return cryptoKeyName;
  }
}
