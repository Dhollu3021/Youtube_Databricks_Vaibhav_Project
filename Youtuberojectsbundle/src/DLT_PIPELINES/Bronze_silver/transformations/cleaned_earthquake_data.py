import dlt
from pyspark.sql.functions import col

# =========================
# SOURCE DATA (BRONZE)
# =========================
@dlt.table(
    name="earthquake_data_bronze"
)
def earthquake_data_bronze():
    return spark.read.format("delta").load(
        "/Volumes/youtube_dev/bronze/earthquake_data"
    )

# =========================
# SILVER LAYER (CLEANED)
# =========================
@dlt.table(
    name="earthquake_data_silver"
)
def earthquake_data_silver():

    df = dlt.read("earthquake_data_bronze")

    # Clean column mapping (fixed version)
    df = df.withColumn("mag", col("properties_mag"))
    df = df.withColumn("time", col("properties_time"))
    df = df.withColumn("updated", col("properties_updated"))
    df = df.withColumn("tsunami", col("properties_tsunami"))
    df = df.withColumn("sig", col("properties_sig"))
    df = df.withColumn("nst", col("properties_nst"))
    df = df.withColumn("dmin", col("properties_dmin"))
    df = df.withColumn("rms", col("properties_rms"))
    df = df.withColumn("gap", col("properties_gap"))

    # Drop original flattened columns
    df = df.drop(
        "properties_mag",
        "properties_time",
        "properties_updated",
        "properties_tsunami",
        "properties_sig",
        "properties_nst",
        "properties_dmin",
        "properties_rms",
        "properties_gap"
    )

    return df