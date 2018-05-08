package org.muks.insider.utils;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SaveMode;

import java.util.HashMap;

public class SparkDbConnectors {


    public static void datasetToCassandra(Dataset dataset,
                                          String keySpace,
                                          String table,
                                          SaveMode saveMode) {

        dataset.write().format("org.apache.spark.sql.cassandra").mode(saveMode)
                .options(new HashMap<String, String>() {
                    {
                        put("keyspace", keySpace);
                        put("table", table);
                    }
                }).save();

    }
}
