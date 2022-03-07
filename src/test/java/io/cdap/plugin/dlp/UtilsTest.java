package io.cdap.plugin.dlp;

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
import io.cdap.cdap.etl.api.validation.ValidationException;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

public class UtilsTest {
  Table.Builder tableBuilder;
  StructuredRecord.Builder recordBuilder;

  @Before
  public void beforeEach() {
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
    config.dlpLocation = "global";
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
    config.dlpLocation = "global";
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
    config.dlpLocation = "global";
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
  public void testEmptyDlpLocation() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.customTemplateEnabled = false;
    config.dlpLocation = "";
    MockFailureCollector collector = new MockFailureCollector("DlpLocation");
    config.validate(collector, null);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("dlpLocation",
                        collector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
    Assert.assertEquals("Resource Location is not specified.",
                        collector.getValidationFailures().get(0).getMessage());
  }

  @Test
  public void testNullDlpLocation() {
    DLPTransformPluginConfig config = new DLPTransformPluginConfig();
    config.customTemplateEnabled = false;
    MockFailureCollector collector = new MockFailureCollector("DlpLocation");
    config.validate(collector, null);
    Assert.assertEquals(1, collector.getValidationFailures().size());
    Assert.assertEquals("dlpLocation",
                        collector.getValidationFailures().get(0).getCauses().get(0).getAttribute("stageConfig"));
    Assert.assertEquals("Resource Location is not specified.",
                        collector.getValidationFailures().get(0).getMessage());
  }
}
