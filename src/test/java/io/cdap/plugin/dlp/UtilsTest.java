package io.cdap.plugin.dlp;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.dlp.v2.DlpServiceClient;
import com.google.cloud.dlp.v2.DlpServiceSettings;
import com.google.privacy.dlp.v2.ByteContentItem;
import com.google.privacy.dlp.v2.ContentItem;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.InfoType;
import com.google.privacy.dlp.v2.InspectConfig;
import com.google.privacy.dlp.v2.InspectContentRequest;
import com.google.privacy.dlp.v2.InspectContentResponse;
import com.google.privacy.dlp.v2.InspectResult;
import com.google.privacy.dlp.v2.Likelihood;
import com.google.privacy.dlp.v2.LocationName;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.ByteString;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import io.cdap.plugin.gcp.common.GCPUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UtilsTest {
  Table.Builder tableBuilder;
  StructuredRecord.Builder recordBuilder;
  static String serviceAccountFilePath;
  static String messageTemplate;
  static DlpServiceSettings dlpServiceSettings;
  static DlpServiceClient dlpServiceClient;
  static String projectId;

  @Before
  public void beforeEach() throws Exception {
    tableBuilder = Table.newBuilder();
    FieldId.Builder firstNameBuilder = FieldId.newBuilder();
    firstNameBuilder.setName("firstName");
    FieldId.Builder lastNameBuilder = FieldId.newBuilder();
    lastNameBuilder.setName("lastName");
    FieldId.Builder dobBuilder = FieldId.newBuilder();
    dobBuilder.setName("dob");
    tableBuilder.addHeaders(firstNameBuilder.build());
    tableBuilder.addHeaders(lastNameBuilder.build());
    tableBuilder.addHeaders(dobBuilder.build());

    LocalDate date = LocalDate.of(2019, 1, 1);

    recordBuilder = StructuredRecord.builder(Schema.recordOf(
        "record",
        Schema.Field.of("firstName", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("lastName", Schema.of(Schema.Type.STRING)),
        Schema.Field.of("dob", Schema.of(LogicalType.DATE))));
    recordBuilder.set("firstName", "John");
    recordBuilder.set("lastName", "Smith");
    recordBuilder.setDate("dob", date);

    Table.Row.Builder rowBuilder = Table.Row.newBuilder();
    Value.Builder johnValueBuilder = Value.newBuilder();
    johnValueBuilder.setStringValue("John");
    Value.Builder smithValueBuilder = Value.newBuilder();
    smithValueBuilder.setStringValue("Smith");
    Value.Builder dateValueBuilder = Value.newBuilder();
    dateValueBuilder.setDateValue(com.google.type.Date.newBuilder().setDay(1).setMonth(1).setYear(2019));
    rowBuilder.addValues(johnValueBuilder);
    rowBuilder.addValues(smithValueBuilder);
    rowBuilder.addValues(dateValueBuilder);

    tableBuilder.addRows(rowBuilder);
  }

  @BeforeClass
  public static void beforeAll() throws Exception {
    //Start DLP Client
    messageTemplate = "%s is not configured, please refer to javadoc of this class for details.";
    serviceAccountFilePath = System.getProperty("service.account.file");
    Assume.assumeFalse(String.format(messageTemplate, "service account key file"), serviceAccountFilePath == null);
    ServiceAccountCredentials serviceAccountCredentials =
        GCPUtils.loadServiceAccountCredentials(serviceAccountFilePath);
    projectId = serviceAccountCredentials.getProjectId();
    DlpServiceSettings.Builder builder = DlpServiceSettings.newBuilder();
    builder.setCredentialsProvider(() -> serviceAccountCredentials);
    dlpServiceSettings = builder.build();
    dlpServiceClient = DlpServiceClient.create(dlpServiceSettings);
  }

  @AfterClass
  public static void afterAll() {
    if (dlpServiceClient != null) {
      dlpServiceClient.shutdown();
    }
  }

  @Test
  public void testStructuredRecordFromTable() throws Exception {
    StructuredRecord structuredRecord = Utils.getStructuredRecordFromTable(tableBuilder.build(), recordBuilder.build());
    Assert.assertEquals(structuredRecord, recordBuilder.build());
  }

  @Test
  public void testTableFromStructuredRecord() throws Exception {
    Table table = Utils.getTableFromStructuredRecord(recordBuilder.build());
    Assert.assertEquals(table, tableBuilder.build());
  }

  @Test
  public void testEmptyCustomTemplate() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.customTemplateEnabled = true;
    config.templateId = null;
    MockFailureCollector collector = new MockFailureCollector("customTemplateEnabledAndEmpty");
    config.validate(collector, null);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("templateId",
                        collector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
    Assert.assertEquals("customTemplatePath",
                        collector.getValidationFailures().get(0).getCauses().get(1).getAttribute("stageConfig"));
    Assert.assertEquals("Custom template fields are not specified.",
                        collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testCustomTemplateEnabled() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.customTemplateEnabled = true;
    config.customTemplatePath = "simple_template";
    MockFailureCollector collector = new MockFailureCollector("customTemplateEnabled");
    config.validate(collector, null);
    Assert.assertEquals(0, collector.getValidationFailures().size());
  }

  @Test
  public void testBothCustomTemplateEnabled() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.customTemplateEnabled = true;
    config.templateId = "template";
    config.customTemplatePath = "anotherTemplate";
    MockFailureCollector collector = new MockFailureCollector("BothCustomTemplateEnabled");
    config.validate(collector, null);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("templateId",
                        collector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
    Assert.assertEquals("customTemplatePath",
                        collector.getValidationFailures().get(0).getCauses().get(1).getAttribute("stageConfig"));
    Assert.assertEquals("Both template id and template path are specified.",
                        collector.getValidationFailures().get(0).getMessage());

  }

  @Test
  public void testLocation() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.location = "europe-west1";
    Assert.assertEquals("europe-west1", config.getLocation());
  }

  @Test
  public void testEmptyLocation() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.location = "";
    Assert.assertEquals("global", config.getLocation());
  }

  @Test
  public void testNullLocation() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.location = null;
    Assert.assertEquals("global", config.getLocation());
  }

  @Test
  public void testDlpConnection() throws Exception {
    Assume.assumeFalse(String.format(messageTemplate, "DLP Service Client"), serviceAccountFilePath == null);
    String text = "His name was Robert Frost";
    ByteContentItem byteContentItem =
      ByteContentItem.newBuilder()
        .setType(ByteContentItem.BytesType.TEXT_UTF8)
        .setData(ByteString.copyFromUtf8(text))
        .build();
    ContentItem contentItem = ContentItem.newBuilder().setByteItem(byteContentItem).build();

    List<InfoType> infoTypes =
      Stream.of("PERSON_NAME", "US_STATE")
        .map(it -> InfoType.newBuilder().setName(it).build())
        .collect(Collectors.toList());

    Likelihood minLikelihood = Likelihood.POSSIBLE;

    InspectConfig.FindingLimits findingLimits =
      InspectConfig.FindingLimits.newBuilder().setMaxFindingsPerItem(0).build();

    InspectConfig inspectConfig =
      InspectConfig.newBuilder()
        .addAllInfoTypes(infoTypes)
        .setMinLikelihood(minLikelihood)
        .setLimits(findingLimits)
        .setIncludeQuote(true)
        .build();

    InspectContentRequest request =
      InspectContentRequest.newBuilder()
        .setParent(LocationName.of(projectId, "global").toString())
        .setInspectConfig(inspectConfig)
        .setItem(contentItem)
        .build();

    InspectContentResponse response = dlpServiceClient.inspectContent(request);

    InspectResult result = response.getResult();

    Assert.assertEquals(true, result.getFindingsList().size() > 0);
  }

}
