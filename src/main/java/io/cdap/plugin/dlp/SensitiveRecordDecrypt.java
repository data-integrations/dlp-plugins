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

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.CryptoDeterministicConfig;
import com.google.privacy.dlp.v2.CryptoReplaceFfxFpeConfig;
import com.google.privacy.dlp.v2.CustomInfoType;
import com.google.privacy.dlp.v2.DeidentifyConfig;
import com.google.privacy.dlp.v2.DeidentifyContentRequest;
import com.google.privacy.dlp.v2.DeidentifyContentResponse;
import com.google.privacy.dlp.v2.Error;
import com.google.privacy.dlp.v2.FieldTransformation;
import com.google.privacy.dlp.v2.GetInspectTemplateRequest;
import com.google.privacy.dlp.v2.InfoTypeTransformations;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectTemplate;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.ReidentifyContentRequest;
import com.google.privacy.dlp.v2.ReidentifyContentResponse;
import com.google.privacy.dlp.v2.Table;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.dlp.configs.DlpFieldTransformationConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Class for the Redact DLP transform plugin
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name(SensitiveRecordDecrypt.NAME)
@Description(SensitiveRecordDecrypt.DESCRIPTION)
public class SensitiveRecordDecrypt extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SensitiveRecordDecrypt.class);
  public static final String NAME = "SensitiveRecordDecrypt";
  public static final String DESCRIPTION = "SensitiveRecordDecrypt";

  private StageMetrics metrics;

  // Stores the configuration passed to this class from user.
  private final Config config;

  // DLP service client for managing interactions with DLP service.
  private DlpServiceClient client;

  // Required fields that need to be sent to DLP for the transform to work, this variable is used to cache the set
  // since it will not change during the execution of the plugin
  private Set<String> requiredFields = null;

  @VisibleForTesting
  public SensitiveRecordDecrypt(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    config.validate(stageConfigurer.getFailureCollector(), stageConfigurer.getInputSchema());

    stageConfigurer.setOutputSchema(stageConfigurer.getInputSchema());
    super.configurePipeline(pipelineConfigurer);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    metrics = context.getMetrics();
    client = DlpServiceClient.create(getSettings());
    try {
      requiredFields = config.getRequiredFields();
    } catch (Exception e) {
      LOG.warn("Unable to get list of required fields, defaulting to an empty set.", e);
      requiredFields = new HashSet<>();
    }
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);
    config.validate(context.getFailureCollector(), context.getInputSchema());
    context.getFailureCollector().getOrThrowException();
    if (config.customTemplateEnabled) {
      String templateName = config.getCustomTemplate();
      GetInspectTemplateRequest request = GetInspectTemplateRequest.newBuilder().setName(templateName).build();

      try {
        if (client == null) {
          client = DlpServiceClient.create(getSettings());
        }
        InspectTemplate template = client.getInspectTemplate(request);
      } catch (Exception e) {
        throw new IllegalArgumentException(
          "Unable to validate template name. Ensure template ID matches the specified ID in DLP. List of defined " +
            "templates can be found at " +
            "https://console.cloud.google.com/security/dlp/landing/configuration/templates/inspect", e);
      }
    }

    List<FieldOperation> fieldOperations = Utils.getFieldOperations(context.getInputSchema(), config, "Decrypt");
    context.record(fieldOperations);
  }

  @Override
  public void transform(StructuredRecord structuredRecord, Emitter<StructuredRecord> emitter) throws Exception {

    RecordTransformations recordTransformations = Utils.constructRecordTransformationsFromConfig(config);
    Table dlpTable = Utils.getTableFromStructuredRecord(structuredRecord, requiredFields);

    DeidentifyConfig deidentifyConfig =
      DeidentifyConfig.newBuilder().setRecordTransformations(recordTransformations).build();

    ContentItem item = ContentItem.newBuilder().setTable(dlpTable).build();
    ReidentifyContentRequest.Builder requestBuilder = ReidentifyContentRequest.newBuilder()
      .setParent(config.getLocationName().toString())
      .setReidentifyConfig(deidentifyConfig)
      .setItem(item);

    // Automatically generating inspection config using the surrogate types defined in the transform
    InspectConfig.Builder configBuilder = InspectConfig.newBuilder();
    for (FieldTransformation fieldTransformation : recordTransformations.getFieldTransformationsList()) {
      for (InfoTypeTransformations.InfoTypeTransformation infoTypeTransformation : fieldTransformation
        .getInfoTypeTransformations().getTransformationsList()) {

        // Only CryptoReplaceFfxFpeConfig and CryptoDeterministicConfig have a surrogate type so target those configs,
        // no other configs should be possible in this transform since they are not included in the widget json list
        // of options
        CryptoReplaceFfxFpeConfig cryptoReplaceFfxFpeConfig = infoTypeTransformation.getPrimitiveTransformation()
          .getCryptoReplaceFfxFpeConfig();
        if (cryptoReplaceFfxFpeConfig.hasCryptoKey()) {
          CustomInfoType customInfoType = CustomInfoType.newBuilder()
            .setInfoType(cryptoReplaceFfxFpeConfig.getSurrogateInfoType())
            .setSurrogateType(CustomInfoType.SurrogateType.newBuilder().build()).build();
          configBuilder.addCustomInfoTypes(customInfoType);
        }

        CryptoDeterministicConfig cryptoDeterministicConfig = infoTypeTransformation.getPrimitiveTransformation()
          .getCryptoDeterministicConfig();
        if (cryptoDeterministicConfig.hasCryptoKey()) {
          CustomInfoType customInfoType = CustomInfoType.newBuilder()
            .setInfoType(cryptoDeterministicConfig.getSurrogateInfoType())
            .setSurrogateType(CustomInfoType.SurrogateType.newBuilder().build()).build();
          configBuilder.addCustomInfoTypes(customInfoType);
        }
      }
    }
    configBuilder.setMinLikelihood(Likelihood.POSSIBLE);
    requestBuilder.setInspectConfig(configBuilder);

    ReidentifyContentResponse response = null;
    ReidentifyContentRequest request = requestBuilder.build();
    try {
      metrics.count("dlp.requests.count", 1);
      response = client.reidentifyContent(request);
    } catch (ApiException e) {
      metrics.count("dlp.requests.fail", 1);
      if (e instanceof ResourceExhaustedException) {
        LOG.error(
          "Failed due to DLP rate limit, please request more quota from DLP: https://cloud.google"
            + ".com/dlp/limits#increases");
      }
      throw e;
    }

    metrics.count("dlp.requests.success", 1);
    ContentItem item1 = response.getItem();
    StructuredRecord resultRecord = Utils.getStructuredRecordFromTable(item1.getTable(), structuredRecord);
    emitter.emit(resultRecord);
  }

  /**
   * Configures the <code>DlpSettings</code> to use user specified service account file or auto-detect.
   *
   * @return Instance of <code>DlpServiceSettings</code>
   * @throws IOException thrown when there is issue reading service account file.
   */
  private DlpServiceSettings getSettings() throws IOException {
    DlpServiceSettings.Builder builder = DlpServiceSettings.newBuilder();
    if (config.getServiceAccountFilePath() != null) {
      builder
        .setCredentialsProvider(() -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
    }
    return builder.build();
  }

  /**
   * Holds configuration required for configuring {@link SensitiveRecordDecrypt}.
   */
  public static class Config extends DLPTransformPluginConfig {
  }
}
