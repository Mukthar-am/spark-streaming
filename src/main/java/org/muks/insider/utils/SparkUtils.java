package org.muks.insider.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SparkUtils {

    public static boolean checkColumnInDataset(Dataset<Row> parsedJson, String columnName) {
        boolean found = false;

        String[] columnNames = parsedJson.toDF().schema().fieldNames();
        for (String column : columnNames) {
            if (column.equalsIgnoreCase(columnName))
                found = true;
        }

        return found;
    }
}
