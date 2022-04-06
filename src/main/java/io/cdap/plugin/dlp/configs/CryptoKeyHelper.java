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
import com.google.common.io.BaseEncoding;
import com.google.gson.Gson;
import com.google.privacy.dlp.v2.CryptoKey;
import com.google.privacy.dlp.v2.KmsWrappedCryptoKey;
import com.google.privacy.dlp.v2.TransientCryptoKey;
import com.google.privacy.dlp.v2.UnwrappedCryptoKey;
import com.google.protobuf.ByteString;
import io.cdap.cdap.etl.api.FailureCollector;

import java.nio.charset.Charset;
import java.util.Base64;
import java.util.Random;


/**
 * Helper class for validating and generating {@link CryptoKey} object for DLP transforms
 */
public class CryptoKeyHelper {

  private static final Gson gson = new Gson();

  /**
   * Three supported types of {@link CryptoKey} in DLP
   */
  public enum KeyType {
    TRANSIENT,
    UNWRAPPED,
    KMS_WRAPPED
  }

  public static CryptoKey createKey(KeyType keyType, String name, String key,
                                    String cryptoKeyName, String wrappedKey) {
    CryptoKey.Builder cryptoKeyBuilder = CryptoKey.newBuilder();
    switch (keyType) {
      case TRANSIENT:
        if (Strings.isNullOrEmpty(name)) {
          //Generate random name for transient key
          byte[] array = new byte[15];
          new Random().nextBytes(array);
          name = new String(array, Charset.forName("UTF-8"));
        }
        cryptoKeyBuilder.setTransient(
          TransientCryptoKey.newBuilder()
            .setName(name)
            .build());
        break;
      case UNWRAPPED:
        cryptoKeyBuilder.setUnwrapped(
          UnwrappedCryptoKey.newBuilder()
            .setKey(ByteString.copyFrom(Base64.getDecoder().decode(key)))
            .build());
        break;
      case KMS_WRAPPED:
        cryptoKeyBuilder.setKmsWrapped(
          KmsWrappedCryptoKey.newBuilder()
            .setCryptoKeyName(cryptoKeyName)
            .setWrappedKey(ByteString.copyFrom(Base64.getDecoder().decode(wrappedKey)))
            .build());
        break;
    }
    return cryptoKeyBuilder.build();
  }

  public static void validateKey(FailureCollector collector, String widgetName, ErrorConfig errorConfig,
                                 KeyType keyType, String name, String key,
                                 String cryptoKeyName, String wrappedKey) {
    if (keyType == null) {
      errorConfig.setNestedTransformPropertyId("keyType");
      collector.addFailure("Crypto Key Type is a required field for this transform.", "Please provide a value.")
        .withConfigElement(widgetName, gson.toJson(errorConfig));
      return;
    }
    switch (keyType) {
      case TRANSIENT:
        break;

      case UNWRAPPED:
        if (Strings.isNullOrEmpty(key)) {
          errorConfig.setNestedTransformPropertyId("key");
          collector.addFailure("Key is a required field for this transform.", "Please provide a value.")
            .withConfigElement(widgetName, gson.toJson(errorConfig));
        } else {
          if (!BaseEncoding.base64().canDecode(key)) {
            errorConfig.setNestedTransformPropertyId("key");
            collector.addFailure("Key must be base64 encoded.",
                                 "").withConfigElement(widgetName, gson.toJson(errorConfig));
          } else {
            byte[] decodedKey = Base64.getDecoder().decode(key);
            if (!(decodedKey.length == 16 || decodedKey.length == 24 || decodedKey.length == 32)) {
              errorConfig.setNestedTransformPropertyId("key");
              collector.addFailure("Key must be 16/24/32 bytes long.",
                                   "Please provide a key of the correct length.")
                .withConfigElement(widgetName, gson.toJson(errorConfig));
            }
          }
        }
        break;
      case KMS_WRAPPED:
        if (Strings.isNullOrEmpty(wrappedKey)) {
          errorConfig.setNestedTransformPropertyId("wrappedKey");
          collector.addFailure("Wrapped Key is a required field for this transform.", "Please provide a value.")
            .withConfigElement(widgetName, gson.toJson(errorConfig));
        }

        if (Strings.isNullOrEmpty(cryptoKeyName)) {
          errorConfig.setNestedTransformPropertyId("cryptoKeyName");
          collector.addFailure("Crypto Key Name is a required field for this transform.", "Please provide a value.")
            .withConfigElement(widgetName, gson.toJson(errorConfig));
        }
        break;
    }
  }
}
