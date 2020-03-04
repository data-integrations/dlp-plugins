package io.cdap.plugin.dlp;

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.Schema.LogicalType;
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
}
