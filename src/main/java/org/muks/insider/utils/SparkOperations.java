package org.muks.insider.utils;

public class SparkOperations {

//                        Dataset joinedDataset
//                                = fullyExploded.join(aggregated, "user_id");

//                        Dataset joinedDataset
//                                = aggregated.join(fullyExploded,
//                                (aggregated.col("user_id").equalTo(fullyExploded.col("user_id"))
//                                        .and(
//                                        aggregated.col("session_id").equalTo(fullyExploded.col("session_id"))
//                                        )
////                                        .and(
////                                                aggregated.col("product_id").equalTo(fullyExploded.col("product_id"))
////                                        )
//                                ),
//                                "inner"
//                        );

//                        Dataset joinedDataset = fullyExploded.join(aggregated, "inner")
//                                .where(
//                                        aggregated.col("user_id").equalTo(fullyExploded.col("user_id"))
//                                                .and(
//                                                        aggregated.col("session_id").equalTo(fullyExploded.col("session_id"))
//                                                )
//                                        .and(aggregated.col("product_id").equalTo(fullyExploded.col("product_id")))
//
//                                );//.select("user_id", "session_id", "product_id", "cart_amount", "product_price", "product_name", "product_url");



    //Dataset joinedDataset = fullyExploded.crossJoin(aggregated);

//                        Dataset joinedDataset
//                                = fullyExploded.join(aggregated)
//                                .where(
//                                        aggregated.col("user_id").equalTo(fullyExploded.col("user_id"))
//                                                .and(
//                                                        aggregated.col("session_id").equalTo(fullyExploded.col("session_id"))
//                                                )
//                                                .and(aggregated.col("session_id").equalTo(fullyExploded.col("date")))
//
//                                );


//                        LOG.info("Show() schema");
//                        aggregated.printSchema();
//                        aggregated.show();
//
}
