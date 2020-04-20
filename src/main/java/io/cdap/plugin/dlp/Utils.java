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

import com.google.common.base.Strings;
import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.RecordTransformations;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.lineage.field.FieldOperation;
import io.cdap.cdap.etl.api.lineage.field.FieldTransformOperation;
import io.cdap.plugin.dlp.configs.DlpFieldTransformationConfig;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Helper class for converting to DLP data types
 */
public final class Utils {

  /**
   * Created updated StructuredRecord from existing record and DLP Table of new values
   * StructuredRecord
   *
   * @param table     Table representation from DLP containing new values to be updated
   * @param oldRecord Existing StructuredRecord that needs to be updated
   * @return New StructuredRecord with the updated values
   * @throws Exception when table contain unexpected type or type-casting fails due to invalid values
   */
  public static StructuredRecord getStructuredRecordFromTable(Table table, StructuredRecord oldRecord)
    throws Exception {
    StructuredRecord.Builder recordBuilder = createBuilderFromStructuredRecord(oldRecord);

    if (table.getRowsCount() == 0) {
      String headers = String.join(", ", table.getHeadersList().stream().map(fieldId -> fieldId.getName()).collect(
        Collectors.toList()));
      throw new IndexOutOfBoundsException(String.format(
        "DLP returned a table with no rows, expected one row. Table has headers: %s.", headers));
    }
    Table.Row row = table.getRows(0);
    for (int i = 0; i < table.getHeadersList().size(); i++) {
      String fieldName = table.getHeadersList().get(i).getName();
      Value fieldValue = row.getValues(i);
      if (fieldValue == null) {
        continue;
      }

      Schema tempSchema = oldRecord.getSchema().getField(fieldName).getSchema();
      Schema fieldSchema = tempSchema.isNullable() ? tempSchema.getNonNullable() : tempSchema;
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null) {
        switch (logicalType) {
          case TIME_MICROS:
          case TIME_MILLIS:
            TimeOfDay timeValue = fieldValue.getTimeValue();
            recordBuilder.setTime(fieldName, LocalTime
              .of(timeValue.getHours(), timeValue.getMinutes(), timeValue.getSeconds(), timeValue.getNanos()));
            break;
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            Timestamp timestampValue = fieldValue.getTimestampValue();
            ZoneId zoneId = oldRecord.getTimestamp(fieldName).getZone();
            LocalDateTime localDateTime;
            if (timestampValue.getSeconds() + timestampValue.getNanos() == 0) {
              localDateTime = LocalDateTime
                .parse(fieldValue.getStringValue(), DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm'Z'"));
            } else {
              localDateTime = Instant
                .ofEpochSecond(timestampValue.getSeconds(), timestampValue.getNanos())
                .atZone(zoneId)
                .toLocalDateTime();
            }
            recordBuilder.setTimestamp(fieldName, ZonedDateTime.of(localDateTime, zoneId));
            break;
          case DATE:
            Date dateValue = fieldValue.getDateValue();
            recordBuilder
              .setDate(fieldName, LocalDate.of(dateValue.getYear(), dateValue.getMonth(), dateValue.getDay()));
            break;
          default:
            throw new IllegalArgumentException("Failed to parse table into structured record");
        }
      } else {
        Schema.Type type = fieldSchema.getType();
        String value = "";
        switch (type) {
          case STRING:
            value = fieldValue.getStringValue();
            break;
          case INT:
          case LONG:
            value = String.valueOf(fieldValue.getIntegerValue());
            break;
          case BOOLEAN:
            value = String.valueOf(fieldValue.getBooleanValue());
            break;
          case DOUBLE:
          case FLOAT:
            value = String.valueOf(fieldValue.getFloatValue());
            break;
          default:
            throw new IllegalStateException(
              String.format("DLP plugin does not support type '%s' for field '%s'", type.toString(), fieldName));
        }
        recordBuilder.convertAndSet(fieldName, value);
      }
    }
    return recordBuilder.build();
  }

  /**
   * Create a StructuredRecord Builder using an existing record as the starting point. This should be used to
   * edit/update an existing record
   *
   * @param record StructuredRecord to use as a base
   * @return StructuredRecord Builder with all fields from {@code record} set
   */
  private static StructuredRecord.Builder createBuilderFromStructuredRecord(StructuredRecord record) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(record.getSchema());
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      Object fieldValue = record.get(fieldName);
      Schema fieldSchema = field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();

      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (fieldSchema.getType().isSimpleType()) {
        recordBuilder.set(fieldName, fieldValue);
      } else {
        if (logicalType != null) {
          switch (logicalType) {
            case TIME_MICROS:
            case TIME_MILLIS:
              recordBuilder.setTime(fieldName, (LocalTime) fieldValue);
              break;
            case TIMESTAMP_MICROS:
            case TIMESTAMP_MILLIS:
              recordBuilder.setTimestamp(fieldName, (ZonedDateTime) fieldValue);
              break;
            case DATE:
              recordBuilder.setDate(fieldName, (LocalDate) fieldValue);
              break;
            default:
              throw new IllegalStateException(
                String.format("DLP plugin does not support type '%s' for field '%s'",
                              logicalType.toString(), field.getSchema().getDisplayName()));
          }
        }
      }
    }
    return recordBuilder;
  }

  /**
   * Creates a DLP Table using the all fields from the given StructuredRecord
   *
   * @param record StructuredRecord to convert to Table
   * @return Table with one row containing the data from {@code record}
   * @throws Exception when record contains unexpected type or type-casting fails due to invalid values
   */
  public static Table getTableFromStructuredRecord(StructuredRecord record) throws Exception {
    return getTableFromStructuredRecord(record, null);
  }

  /**
   * Creates a DLP Table using the required fields from StructuredRecord
   *
   * @param record         StructuredRecord to convert to Table
   * @param requiredFields Set of fields to include in the conversion, pass empty set to include all fields
   * @return Table with one row containing the data from {@code record}
   * @throws Exception when record contains unexpected type or type-casting fails due to invalid values
   */
  public static Table getTableFromStructuredRecord(StructuredRecord record, @Nullable Set<String> requiredFields)
    throws Exception {
    Table.Builder tableBuiler = Table.newBuilder();
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();

    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      if (requiredFields != null && requiredFields.size() > 0 && !requiredFields.contains(fieldName)) {
        continue;
      }
      Object fieldValue = record.get(fieldName);
      if (fieldValue == null) {
        continue;
      }

      tableBuiler.addHeaders(FieldId.newBuilder().setName(fieldName).build());
      Value.Builder valueBuilder = Value.newBuilder();
      Schema fieldSchema =
        field.getSchema().isNullable() ? field.getSchema().getNonNullable() : field.getSchema();
      Schema.LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType != null) {
        switch (logicalType) {
          case TIME_MICROS:
          case TIME_MILLIS:
            LocalTime time = record.getTime(fieldName);
            valueBuilder.setTimeValue(
              TimeOfDay.newBuilder()
                .setHours(time.getHour())
                .setMinutes(time.getMinute())
                .setSeconds(time.getSecond())
                .setNanos(time.getNano())
                .build()
            );
            break;
          case TIMESTAMP_MICROS:
          case TIMESTAMP_MILLIS:
            ZonedDateTime timestamp = record.getTimestamp(fieldName);
            valueBuilder.setTimestampValue(
              Timestamp.newBuilder()
                .setSeconds(timestamp.toEpochSecond())
                .setNanos(timestamp.getNano())
                .build()
            );
            break;
          case DATE:
            LocalDate date = record.getDate(fieldName);
            valueBuilder.setDateValue(
              Date.newBuilder()
                .setYear(date.getYear())
                .setMonth(date.getMonthValue())
                .setDay(date.getDayOfMonth())
                .build()
            );
            break;
          default:
            throw new IllegalStateException(
              String.format("DLP plugin does not support type '%s' for field '%s'", logicalType.toString(), fieldName));
        }
      } else {

        Schema.Type type = fieldSchema.getType();
        switch (type) {
          case STRING:
            valueBuilder.setStringValue(String.valueOf(fieldValue));
            break;
          case INT:
          case LONG:
            valueBuilder.setIntegerValue((long) fieldValue);
            break;
          case BOOLEAN:
            valueBuilder.setBooleanValue((Boolean) fieldValue);
            break;
          case DOUBLE:
          case FLOAT:
            valueBuilder.setFloatValue((double) fieldValue);
            break;
          default:
            throw new IllegalStateException(
              String.format("DLP plugin does not support type '%s' for field '%s'", type.toString(), fieldName));
        }
      }

      rowBuilder.addValues(valueBuilder.build());
    }
    tableBuiler.addRows(rowBuilder.build());
    return tableBuiler.build();
  }

  /**
   * Converts the internal plugin config representation to DLP {@link RecordTransformations} object
   *
   * @param config DLP transform plugin config
   * @return DLP RecordTransformations object that contains the converted values
   * @throws Exception if the config cannot be converted into RecordTransformations
   */
  protected static RecordTransformations constructRecordTransformationsFromConfig(DLPTransformPluginConfig config)
    throws Exception {
    RecordTransformations.Builder recordTransformationsBuilder = RecordTransformations.newBuilder();
    List<DlpFieldTransformationConfig> transformationConfigs = config.parseTransformations();

    recordTransformationsBuilder.addAllFieldTransformations(
      transformationConfigs.stream()
        .map(DlpFieldTransformationConfig::toFieldTransformation)
        .collect(Collectors.toList())
    );
    return recordTransformationsBuilder.build();
  }

  /**
   * Constructs the field operations list required for Field Level Lineage in DLP transform plugins (Redact and
   * Decrypt)
   *
   * @param inputSchema The schema of records coming into the transform
   * @param config      The DLP transform plugin config for the transform
   * @return List of {@link FieldOperation} that contains the data for FLL
   * @throws Exception if the config cannot be parsed to obtain the list of transformations
   */
  protected static List<FieldOperation> getFieldOperations(Schema inputSchema, DLPTransformPluginConfig config)
    throws Exception {
    return getFieldOperations(inputSchema, config, "");
  }

  /**
   * Constructs the field operations list required for Field Level Lineage in DLP transform plugins (Redact and
   * Decrypt)
   *
   * @param inputSchema         The schema of records coming into the transform
   * @param config              The DLP transform plugin config for the transform
   * @param transformNamePrefix String prefix to add to the transform names to differentiate between Redact and Decrypt
   * @return List of {@link FieldOperation} that contains the data for FLL
   * @throws Exception if the config cannot be parsed to obtain the list of transformations
   */
  protected static List<FieldOperation> getFieldOperations(Schema inputSchema, DLPTransformPluginConfig config,
                                                           String transformNamePrefix) throws Exception {
    if (!Strings.isNullOrEmpty(transformNamePrefix) && !transformNamePrefix.endsWith(" ")) {
      transformNamePrefix += " ";
    }

    //Parse config into format 'FieldName': List<>(['transform','filter'])
    HashMap<String, List<String[]>> fieldOperationsData = new HashMap<>();
    for (DlpFieldTransformationConfig transformationConfig : config.parseTransformations()) {
      for (String field : transformationConfig.getFields()) {
        String filterName = String.join(", ", transformationConfig.getFilters())
          .replace("NONE", String.format("Custom Template (%s)", config.templateId));

        String transformName = transformNamePrefix + transformationConfig.getTransform();

        if (!fieldOperationsData.containsKey(field)) {
          fieldOperationsData.put(field, Collections.singletonList(new String[]{transformName, filterName}));
        } else {
          fieldOperationsData.get(field).add(new String[]{transformName, filterName});
        }
      }
    }

    for (Schema.Field field : inputSchema.getFields()) {
      if (!fieldOperationsData.containsKey(field.getName())) {
        fieldOperationsData.put(field.getName(), Collections.singletonList(new String[]{"Identity", ""}));
      }
    }

    List<FieldOperation> fieldOperations = new ArrayList<>();
    for (String fieldName : fieldOperationsData.keySet()) {
      StringBuilder descriptionBuilder = new StringBuilder();
      StringBuilder nameBuilder = new StringBuilder();
      descriptionBuilder.append("Applied ");
      boolean first = true;
      for (String[] transformFilterPair : fieldOperationsData.get(fieldName)) {

        String transformName = transformFilterPair[0];
        String filterNames = transformFilterPair[1];

        if (first) {
          descriptionBuilder.append("        ");
        }
        descriptionBuilder.append(String.format("'%s' transform on contents ", transformName));
        if (filterNames.length() > 0) {
          descriptionBuilder.append(" matching ").append(filterNames);
        }
        descriptionBuilder.append(",\n");
        nameBuilder.append(transformName).append(" ,");
        first = false;
      }
      nameBuilder.deleteCharAt(nameBuilder.length() - 1);
      descriptionBuilder.delete(descriptionBuilder.length() - 2, descriptionBuilder.length() - 1);
      nameBuilder.append("on ").append(fieldName);
      fieldOperations
        .add(new FieldTransformOperation(nameBuilder.toString(), descriptionBuilder.toString(),
                                         Collections.singletonList(fieldName), fieldName));
    }
    return fieldOperations;
  }
}
