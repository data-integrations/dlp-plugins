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

import com.google.privacy.dlp.v2.FieldId;
import com.google.privacy.dlp.v2.Table;
import com.google.privacy.dlp.v2.Value;
import com.google.protobuf.Timestamp;
import com.google.type.Date;
import com.google.type.TimeOfDay;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Helper class for converting to DLP data types
 */
public final class Utils {

  /**
   * Created updated StructuredRecord from existing record and DLP Table of new values
   * StructuredRecord
   *
   * @param table     Table representation from DLP containing new values to be updated
   * @param oldRecord Exisiting StructuredRecord that needs to be updated
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
        recordBuilder.convertAndSet(fieldName, fieldValue.getStringValue());
      }
    }
    return recordBuilder.build();
  }

  /**
   * Create a StructuredRecord Builder using an exisitng record as the starting point. This should be used to
   * edit/update an exisitng record
   *
   * @param record StructuredRecord to use as a base
   * @return StrucutredRecord Builder with all fields from {@code record} set
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
   * Creates a DLP Table from a StructuredRecord
   *
   * @param record StructuredRecord to convert to Table
   * @return Table with one row containing the data from {@code record}
   * @throws Exception when record contains unexpected type or type-casting fails due to invalid values
   */
  public static Table getTableFromStructuredRecord(StructuredRecord record) throws Exception {
    Table.Builder tableBuiler = Table.newBuilder();
    Table.Row.Builder rowBuilder = Table.Row.newBuilder();

    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
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
            valueBuilder.setIntegerValue((Long) fieldValue);
            break;
          case BOOLEAN:
            valueBuilder.setBooleanValue((Boolean) fieldValue);
            break;
          case DOUBLE:
          case FLOAT:
            valueBuilder.setFloatValue((Double) fieldValue);
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

}
