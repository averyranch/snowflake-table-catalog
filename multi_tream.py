import logging
from pyspark.sql import SparkSession, DataFrame

# Configure Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class StreamingPipeline:
    """
    A structured streaming pipeline supporting multiple sources: Fake Stream (Rate), Kafka, or Flink.
    """

    def __init__(self, spark: SparkSession, source_type: str = "fake", rows_per_sec: int = 10, kafka_topic: str = "assets"):
        """
        Initializes the Streaming Pipeline.

        :param spark: SparkSession object.
        :param source_type: The streaming source ("fake", "kafka", "flink").
        :param rows_per_sec: Rate of records per second (only for 'fake' source).
        :param kafka_topic: Kafka topic name (only for 'kafka' source).
        """
        self.spark = spark
        self.source_type = source_type
        self.rows_per_sec = rows_per_sec
        self.kafka_topic = kafka_topic
        self.raw_stream = self._ingest_stream()

    def _ingest_stream(self) -> DataFrame:
        """
        Ingests streaming data from a specified source.

        :return: Streaming DataFrame.
        """
        logger.info(f"Ingesting data stream from source: {self.source_type}")

        if self.source_type == "fake":
            return self.spark.readStream.format("rate").option("rowsPerSecond", self.rows_per_sec).load()
        
        elif self.source_type == "kafka":
            return (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", self.kafka_topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr("CAST(value AS STRING) as message")  # Convert Kafka binary to string
            )

        elif self.source_type == "flink":
            return (
                self.spark.readStream.format("flink")
                .option("flink.server.address", "localhost:8081")
                .option("stream.name", self.kafka_topic)
                .load()
            )

        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")

    def _write_to_console(self, stream_df: DataFrame):
        """
        Writes streaming data to the console.
        """
        logger.info("Writing streaming data to console...")
        return (
            stream_df.writeStream
            .outputMode("append")
            .format("console")
            .trigger(processingTime="5 seconds")
            .start()
        )

    def run_pipeline(self):
        """
        Runs the full structured streaming pipeline.
        """
        logger.info("Starting Streaming Data Pipeline...")

        query_console = self._write_to_console(self.raw_stream)

        try:
            logger.info("Streaming started... Press Ctrl+C to stop.")
            query_console.awaitTermination()
        except KeyboardInterrupt:
            logger.warning("Stopping Streaming Pipeline...")
            query_console.stop()
            logger.info("Streaming Pipeline Stopped Gracefully.")


def main():
    """
    Entry point for running the structured streaming pipeline.
    """
    spark = SparkSession.builder.appName("StreamingPipeline").getOrCreate()
    
    # Replace "fake" with "kafka" or "flink" to switch streaming sources
    streaming_pipeline = StreamingPipeline(spark, source_type="fake", kafka_topic="assets")
    streaming_pipeline.run_pipeline()


if __name__ == "__main__":
    main()
