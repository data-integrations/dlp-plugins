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

package io.cdap.plugin.gcp.common;

import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.plugin.common.batch.ConfigurationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.OutputFormat;

import java.util.Map;

/**
 * An implementation of {@link OutputFormatProvider} based on {@link Configuration} and {@link OutputFormat} class.
 */
public final class SourceOutputFormatProvider implements OutputFormatProvider {
  private final String outputFormatClassName;
  private final Map<String, String> configuration;

  public SourceOutputFormatProvider(Class<? extends OutputFormat> inputFormatClass, Configuration hConf) {
    this(inputFormatClass.getName(), hConf);
  }

  public SourceOutputFormatProvider(String outputFormatClassName, Configuration hConf) {
    this.outputFormatClassName = outputFormatClassName;
    configuration = ConfigurationUtils.getNonDefaultConfigurations(hConf);
  }

  @Override
  public String getOutputFormatClassName() {
    return outputFormatClassName;
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return configuration;
  }
}
