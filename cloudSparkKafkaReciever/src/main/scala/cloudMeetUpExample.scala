
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j._

object cloudMeetUpExample {
  val spark = SparkSession.builder()
    .appName("Stream processing application")
    //.master("local[*]")
    .getOrCreate()

  val mysql_host_name = "localhost"
  val mysql_port_no = "3306"
  val mysql_user_name = "root"
  val mysql_password = "root"
  val mysql_database_name = "meetup_rsvp_db"
  val mysql_driver_class = "com.mysql.jdbc.Driver"
  val mysql_table_name = "meetup_rsvp_message_agg_detail_tbl"
  val mysql_jdbc_url = "jdbc:mysql://" + mysql_host_name + ":" + mysql_port_no + "/" + mysql_database_name

  val mongodb_host_name = "localhost"
  val mongodb_port_no = "27017"
  val mongodb_user_name = "admin"
  val mongodb_password = "admin"
  val mongodb_database_name = "meetup_rsvp_db"
  val mongodb_collection_name = "meetup_rsvp_message_detail_tbl"

  val meetup_rsvp_message_schema = StructType(Array(
    StructField("venue", StructType(Array(
      StructField("venue_name", StringType),
      StructField("lon", StringType),
      StructField("lat", StringType),
      StructField("venue_id", StringType)
    ))),
    StructField("visibility", StringType),
    StructField("response", StringType),
    StructField("guests", StringType),
    StructField("member", StructType(Array(
      StructField("member_id", StringType),
      StructField("photo", StringType),
      StructField("member_name", StringType)
    ))),
    StructField("rsvp_id", StringType),
    StructField("mtime", StringType),
    StructField("event", StructType(Array(
      StructField("event_name", StringType),
      StructField("event_id", StringType),
      StructField("time", StringType),
      StructField("event_url", StringType)
    ))),
    StructField("group", StructType(Array(
      StructField("group_topics", ArrayType(StructType(Array(
        StructField("urlkey", StringType),
        StructField("topic_name", StringType)
      )), true)),
      StructField("group_city", StringType),
      StructField("group_country", StringType),
      StructField("group_id", StringType),
      StructField("group_name", StringType),
      StructField("group_lon", StringType),
      StructField("group_urlname", StringType),
      StructField("group_state", StringType),
      StructField("group_lat", StringType)
    )))
  ))

  def readFromKafka() = {
    // https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html
    val kafkaDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka-2.us-east4-c.c.ancient-dragon-273016.internal:9092," +
        "kafka-0.us-east4-c.c.ancient-dragon-273016.internal:9092")
      .option("subscribe", "meetup")
      .option("startingOffsets", "latest")
      .load()

    println("Printing Schema of transaction_detail_df: ")
    kafkaDF.printSchema()

    val meetup_rsvp_df_1 = kafkaDF.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")

    val meetup_rsvp_df_2 = meetup_rsvp_df_1.select(from_json(col("value"), meetup_rsvp_message_schema)
      .as("message_detail"), col("timestamp"))

    val meetup_rsvp_df_3 = meetup_rsvp_df_2.select("message_detail.*", "timestamp")

    val meetup_rsvp_df_4 = meetup_rsvp_df_3.select(col("group.group_name"),
      col("group.group_country"), col("group.group_state"), col("group.group_city"),
      col("group.group_lat"), col("group.group_lon"), col("group.group_id"),
      col("group.group_topics"), col("member.member_name"), col("response"),
      col("guests"), col("venue.venue_name"), col("venue.lon"), col("venue.lat"),
      col("venue.venue_id"), col("visibility"), col("member.member_id"),
      col("member.photo"), col("event.event_name"), col("event.event_id"),
      col("event.time"), col("event.event_url")
    )

    println("Printing Schema of meetup_rsvp_df_4: ")
    meetup_rsvp_df_4.printSchema()

//    meetup_rsvp_df_4
//          .writeStream
//          .format("console")
//          .outputMode("append")
//         .start()
//         .awaitTermination()


    val spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
    println("Printing spark_mongodb_output_uri: " + spark_mongodb_output_uri)


    meetup_rsvp_df_4.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database

        batchDF_1.write
          .format("mongo")
          .mode("append")
          .option("uri", spark_mongodb_output_uri)
          .option("database", mongodb_database_name)
          .option("collection", mongodb_collection_name)
          .save()
      }.start()


//    val meetup_rsvp_df_5 = meetup_rsvp_df_4.groupBy("group_name", "group_country",
//      "group_state", "group_city", "group_lat", "group_lon", "response")
//      .agg(count(col("response")).as("response_count"))
//
//    println("Printing Schema of meetup_rsvp_df_5: ")
//    meetup_rsvp_df_5.printSchema()

    val meetup_rsvp_df_5 = meetup_rsvp_df_4.groupBy("group_name", "group_country",
      "group_state", "group_city", "group_lat", "group_lon", "response")
      .agg(count(col("response")).as("response_count"))

    println("Printing Schema of meetup_rsvp_df_5: ")
    meetup_rsvp_df_5.printSchema()

    val trans_detail_write_stream = meetup_rsvp_df_5
      .writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("complete")
      .option("truncate", "false")
      .format("console")
      .start()


    val mysql_properties = new java.util.Properties
    mysql_properties.setProperty("driver", mysql_driver_class)
    mysql_properties.setProperty("user", mysql_user_name)
    mysql_properties.setProperty("password", mysql_password)

    println("mysql_jdbc_url: " + mysql_jdbc_url)
//
    meetup_rsvp_df_5.writeStream
      .trigger(Trigger.ProcessingTime("20 seconds"))
      .outputMode("update")
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        val batchDF_1 = batchDF.withColumn("batch_id", lit(batchId))
        // Transform batchDF and write it to sink/target/persistent storage
        // Write data from spark dataframe to database
        batchDF_1.write.mode("append").jdbc(mysql_jdbc_url, mysql_table_name, mysql_properties)
      }.start()


    trans_detail_write_stream.awaitTermination()

    println("Stream Processing Application Completed.")







//    val json_converted = kafkaDF.select($"value" cast "string" as "json")
//      .select(from_json($"json", meetup_rsvp_message_schema) as "data")
//      .select("data.*")



//    json_converted
//      .writeStream
//      .format("console")
//      .outputMode("append")
//      .start()
//      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    readFromKafka()

  }


}
