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

import com.google.privacy.dlp.v2.CryptoHashConfig;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.PrimitiveTransformation;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Arrays;
import java.util.List;

/**
 * Implementing the DlpTransformConfig interface for the DLP {@link CryptoHashConfig}
 */
public class CryptoHashTransformationConfig implements DlpTransformConfig {

  private String key;
  private String name;
  private String wrappedKey;
  private String cryptoKeyName;
  private CryptoKeyHelper.KeyType keyType;
  private final Schema.Type[] supportedTypes = new Schema.Type[]{Schema.Type.STRING};

  @Override
  public PrimitiveTransformation toPrimitiveTransform() {

    CryptoKey cryptoKey = CryptoKeyHelper.createKey(keyType, name, key, cryptoKeyName, wrappedKey);
    return PrimitiveTransformation.newBuilder()
      .setCryptoHashConfig(
        CryptoHashConfig.newBuilder()
          .setCryptoKey(cryptoKey)
          .build())
      .build();
  }


  @Override
  public void validate(FailureCollector collector, String widgetName, ErrorConfig errorConfig) {
    CryptoKeyHelper.validateKey(collector, widgetName, errorConfig, keyType, name, key, cryptoKeyName, wrappedKey);
  }

  @Override
  public List<Schema.Type> getSupportedTypes() {
    return Arrays.asList(supportedTypes);
  }
}
