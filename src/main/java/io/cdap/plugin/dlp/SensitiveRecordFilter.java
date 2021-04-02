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

package io.cdap.plugin.dlp;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.ResourceExhaustedException;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.common.base.Strings;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.GetInspectTemplateRequest;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.InspectResult;
import com.google.privacy.dlp.v2.InspectTemplate;
import com.google.privacy.dlp.v2.ProjectName;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.MultiOutputEmitter;
import io.cdap.cdap.etl.api.MultiOutputPipelineConfigurer;
import io.cdap.cdap.etl.api.MultiOutputStageConfigurer;
import io.cdap.cdap.etl.api.SplitterTransform;
import io.cdap.cdap.etl.api.StageMetrics;
import io.cdap.cdap.etl.api.StageSubmitterContext;
import io.cdap.cdap.etl.api.TransformContext;
import io.cdap.cdap.format.StructuredRecordStringConverter;
import io.cdap.plugin.gcp.common.GCPConfig;
import io.cdap.plugin.gcp.common.GCPUtils;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * This class <code>SensitiveRecordFilter</code> provides an easy way to filter sensitive PII data from stream. The
 * class utilizes Data Loss Prevention APIs for identifying sensitive data. Depending on the filter confidence set by
 * user, the class either sends input record on sensitive port or non-sensitive port.
 *
 * <p>
 * In case of issue with invoking DLP, the plugin depending on user choice either chooses to skip record, error pipeline
 * or send record to error port.
 * </p>
 */
@Plugin(type = SplitterTransform.PLUGIN_TYPE)
@Name(SensitiveRecordFilter.NAME)
@Description(SensitiveRecordFilter.DESCRIPTION)
public final class SensitiveRecordFilter extends SplitterTransform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(SensitiveRecordFilter.class);
  public static final String NAME = "SensitiveRecordFilter";
  public static final String DESCRIPTION = "Filters input records based that are sensitive.";
  private static final String SENSITIVE_PORT = "Sensitive";
  private static final String NON_SENSITIVE_PORT = "Non-Sensitive";

  // Stores the configuration passed to this class from user.
  private final Config config;
  private StageMetrics metrics;

  // DLP service client for managing interactions with DLP service.
  private DlpServiceClient client;

  public SensitiveRecordFilter(Config config) {
    this.config = config;
  }

  /**
   * Invoked during deployment of pipeline to validate configuration of the pipeline. This method checks if the input
   * specified is 'field' type and if it is, then checks if the field specified is present in the input schema.
   *
   * @param configurer a <code>MultiOutputPipelineConfigurer</code> for configuring pipeline.
   * @throws IllegalArgumentException if there any issues with configuration of the plugin.
   */
  @Override
  public void configurePipeline(MultiOutputPipelineConfigurer configurer) {
    super.configurePipeline(configurer);

    MultiOutputStageConfigurer stageConfigurer = configurer.getMultiOutputStageConfigurer();
    Schema inputSchema = stageConfigurer.getInputSchema();

    try {

      client = DlpServiceClient.create(getSettings());
    } catch (IOException e) {
      LOG.error("Failed to create DLP Client: {}", e.getMessage(), e);
      stageConfigurer.getFailureCollector().addFailure("Failed to create DLP client: " + e.getMessage(), "");
    }

    config.validate(stageConfigurer.getFailureCollector(), inputSchema);

    Map<String, Schema> outputs = new HashMap<>();
    outputs.put(SENSITIVE_PORT, inputSchema);
    outputs.put(NON_SENSITIVE_PORT, inputSchema);
    stageConfigurer.setOutputSchemas(outputs);
  }

  /**
   * Initialize this <code>SensitiveRecordFilter</code> plugin. A instance of DLP client is created with mapped
   * infotypes.
   *
   * @param context Initialization context
   */
  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);

    client = DlpServiceClient.create(getSettings());
    metrics = context.getMetrics();
  }

  @Override
  public void prepareRun(StageSubmitterContext context) throws Exception {
    super.prepareRun(context);

    String templateName = config.getCustomTemplate();
    GetInspectTemplateRequest request = GetInspectTemplateRequest.newBuilder().setName(templateName).build();

    try {
      InspectTemplate template = client.getInspectTemplate(request);
    } catch (Exception e) {
      throw new IllegalArgumentException(
        "Unable to validate template name. Ensure template ID matches the specified ID in DLP");
    }
  }

  /**
   * Splitter Transform splits the sensitive and non-sensitive record into different ports. If user has selected entire
   * record to be checked for sensitive data, then all the fields are concacted as string and passed  to data loss
   * prevention API.
   *
   * @param record  a <code>StructuredRecord</code> being passed from the previous stage.
   * @param emitter a <code>MultiOutputEmitter</code> to emit sensitive or non-sensitive data on different ports.
   */
  @Override
  public void transform(StructuredRecord record, MultiOutputEmitter<StructuredRecord> emitter) throws Exception {
    String recordString = null;
    ContentItem contentItem = null;

    if (!config.entireRecord) {
      recordString = record.get(config.getFieldName()).toString();
    } else {
      recordString = StructuredRecordStringConverter.toDelimitedString(record, ",");
    }
    try {

      // depending on input schema field object
      contentItem = ContentItem.newBuilder().setValue(recordString).build();

      String templateName = config.getCustomTemplate();

      InspectContentRequest request =
        InspectContentRequest.newBuilder()
          .setParent(ProjectName.of(config.getProject()).toString())
          .setInspectTemplateName(templateName)
          .setItem(contentItem)
          .build();

      //Record metrics for how many requests were sent to DLP
      metrics.count("dlp.requests.count", 1);
      InspectContentResponse response = client.inspectContent(request);
      InspectResult result = response.getResult();

      //Record metrics for how many requests were successfully processed by DLP
      metrics.count("dlp.requests.success", 1);
      if (result.getFindingsList().size() > 0) {
        emitter.emit(SENSITIVE_PORT, record);
        return;
      }

      emitter.emit(NON_SENSITIVE_PORT, record);

    } catch (Exception e) {
      //Record metrics for how many requests failed (either DLP error or network error)
      metrics.count("dlp.requests.fail", 1);
      if (e instanceof ResourceExhaustedException) {
        ResourceExhaustedException e1 = (ResourceExhaustedException) e;
        throw new ResourceExhaustedException(
          "Failed due to DLP rate limit, please request more quota from DLP: https://cloud.google"
            + ".com/dlp/limits#increases", e1.getCause(), e1.getStatusCode(), e1.isRetryable());
      }

      switch (config.onErrorHandling()) {
        case -1:
          throw e;
        case 1:
          emitter.emitError(new InvalidEntry<>(-1, e.getMessage(), record));
          break;
      }
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    if (client != null) {
      client.close();
      client = null;
    }
  }


  /**
   * Configures the <code>DlpSettings</code> to use user specified service account file or auto-detect.
   *
   * @return Instance of <code>DlpServiceSettings</code>
   * @throws IOException thrown when there is issue reading service account file.
   */
  private DlpServiceSettings getSettings() throws IOException {
    DlpServiceSettings.Builder builder = DlpServiceSettings.newBuilder();
    if (config.dlpOverrideEnabled) {
      String target = config.dlpHost.trim() + ":" + config.dlpPort;
      if (config.dlpTlsEnabled) {
        builder.setTransportChannelProvider(
            FixedTransportChannelProvider.create(
                GrpcTransportChannel.create(
                    ManagedChannelBuilder.forTarget(target)
                        .useTransportSecurity()
                        .build())));
        builder.setCredentialsProvider(() ->
            GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
      } else {
        builder.setTransportChannelProvider(
            FixedTransportChannelProvider.create(
                GrpcTransportChannel.create(
                    ManagedChannelBuilder.forTarget(target)
                        .usePlaintext()
                        .build())));
        builder.setCredentialsProvider(NoCredentialsProvider.create());
      }
    } else {
      if (config.getServiceAccountFilePath() != null) {
        builder.setCredentialsProvider(
            () -> GCPUtils.loadServiceAccountCredentials(config.getServiceAccountFilePath()));
      }
    }
    return builder.build();
  }


  /**
   * Configuration object.
   */
  public static class Config extends GCPConfig {

    public static final String FIELD = "field";
    public static final String DLP_HOST = "dlp-host";
    public static final String DLP_PORT = "dlp-port";
    public static final String DLP_TLS_ENABLED = "dlp-tls-enabled";
    public static final String CUSTOM_TEMPLATE_PATH_NAME = "customTemplatePath";
    public static final String TEMPLATE_ID_NAME = "template-id";
    public static final String CUSTOM_TEMPLATE_ENABLED_NAME = "customTemplateEnabled";

    @Macro
    @Name("entire-record")
    @Description("Check full record or a field")
    private Boolean entireRecord;

    @Description("Enabling this option will allow you to define a custom DLP Inspection Template to use for matching "
      + "during the transform.")
    protected Boolean customTemplateEnabled;

    @Macro
    @Name(FIELD)
    @Description("Name of field to be inspected")
    @Nullable
    private String field;

    @Macro
    @Name("template-id")
    @Description("ID of the Inspection Template defined in DLP")
    @Nullable
    private String templateId;

    @Macro
    @Name("customTemplatePath")
    @Description("Custom path of the DLP Inspection template")
    @Nullable
    private String customTemplatePath;

    @Macro
    @Name("on-error")
    @Description("Error handling of record")
    private String onError;

    @Macro
    @Name("dlp-override-enabled")
    @Description("Use custom DLP service endpoint")
    private Boolean dlpOverrideEnabled;

    @Macro
    @Name(DLP_HOST)
    @Nullable
    @Description("DLP host, e.g. dlp.googleapis.com or dlp.local")
    private String dlpHost;

    @Macro
    @Name(DLP_PORT)
    @Nullable
    @Description("DLP port number, between 0 and 65535")
    private Integer dlpPort;

    @Macro
    @Name(DLP_TLS_ENABLED)
    @Nullable
    @Description("Enable send credentials if you are accessing the Cloud DLP API through a proxy "
        + "however it is optional if you are using a local instance of DLP.")
    private Boolean dlpTlsEnabled;

    /**
     * @return The name of field that needs to be inspected for sensitive data.
     */
    public String getFieldName() {
      return field;
    }

    /**
     * @return -1 to stop processing, 0 to skip record, 1 to emit record to error.
     */
    public int onErrorHandling() {
      if (onError.equalsIgnoreCase("stop-on-error")) {
        return -1;
      } else if (onError.equalsIgnoreCase("skip-record")) {
        return 0;
      }
      return 1;
    }

    /**
     * Gets the custom template specified by user which can be either a templateId or a full custom template path
     *
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
      if (!containsMacro("entire-record") && !entireRecord && getFieldName() == null) {
        collector.addFailure("Input type is specified as 'Field', " +
                               "but a field name has not been specified.", "Specify the field name.")
          .withConfigProperty(FIELD);
      }

      if (!entireRecord) {
        if (!containsMacro(FIELD)) {
          if (inputSchema.getField(getFieldName()) == null) {
            collector.addFailure("Field specified is not present in the input schema.",
                                 "Update the field or input schema to ensure they match.")
              .withConfigProperty(FIELD);
          }

          Schema fieldSchema = inputSchema.getField(getFieldName()).getSchema();
          Schema.Type type = fieldSchema.isNullable() ? fieldSchema.getNonNullable().getType() :
            fieldSchema.getType();
          if (!type.isSimpleType() || type.equals(Schema.Type.BYTES)) {
            collector.addFailure("Filtering on field supports only basic types " +
                                   "(string, bool, int, long, float, double).",
                                 "Please check that the select field's schema matches those types")
              .withConfigProperty(FIELD);
          }
        }
      }

      if (dlpOverrideEnabled) {
        if (dlpHost == null || dlpHost.isEmpty()) {
          collector.addFailure("Custom DLP endpoint is enabled, " +
                  "but DLP host has not been specified.",
              "Specify the DLP host.")
              .withConfigProperty(DLP_HOST);
        }
        if (dlpPort == null) {
          collector.addFailure("Custom DLP endpoint is enabled, " +
                  "but DLP port has not been specified.",
              "Specify the DLP port.")
              .withConfigProperty(DLP_PORT);
        } else if (dlpPort < 0 || dlpPort > 65535) {
          collector.addFailure("Invalid port.",
              "Change port to a number between 0 and 65535.")
              .withConfigProperty(DLP_PORT);
        }
        if (dlpTlsEnabled == null) {
          collector.addFailure("Custom DLP endpoint is enabled, " +
              "but TLS encryption has not been specified.", "Specify the TLS encryption.")
              .withConfigProperty(DLP_TLS_ENABLED);
        }
      }
    }
  }
}
