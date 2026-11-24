
import os
from datetime import date

import jdatetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, DecimalType

from pathlib import Path
from dotenv import load_dotenv

# Find the project root
BASE_DIR = Path(__file__).resolve().parents[2]

# Load environment variables
env_path = BASE_DIR / ".env"

if not env_path.is_file():
    raise FileNotFoundError(f".env file not found: {env_path}")

load_dotenv(dotenv_path=env_path)

print("Environment variables loaded successfully")

# Configure SQL Server JDBC
jdbc_jar = r"C:\SparkLab\jdbc\mssql-jdbc.jar"

spark = (
    SparkSession.builder
    .appName("HospitalDW_InitialLoad_Radiology")
    .master("local[4]")
    .config("spark.driver.extraClassPath", jdbc_jar)
    .config("spark.executor.extraClassPath", jdbc_jar)
    .getOrCreate()
)

jdbc_url = (
    f"jdbc:sqlserver://{os.environ['SQL_SERVER']}:"
    f"{os.getenv('SQL_PORT', '1433')};"
    "databaseName=dayclinic;"
    "encrypt=true;trustServerCertificate=true"
)

# Trusting the server certificate is for local development only.
connection = {
    "url": jdbc_url,
    "user": os.environ["SQL_USERNAME"],
    "password": os.environ["SQL_PASSWORD"],
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# Extract active receptions and resolve dimension keys.
# Original patient identifiers remain in SQL Server.
source_query = """
(
    SELECT
        r.Turnid AS SourceVisitID,
        r.Turndate,
        r.cash,
        r.PaientType,
        r.Shiftcode AS ShiftCode,
        r.PaientStatus AS PatientStatus,
        p.PatientKey,
        s.ServiceKey,
        pt.PatientTypeKey
    FROM dayclinic.dbo.Radio_Recipe AS r
    INNER JOIN HospitalDW.etl.PatientMap AS pm
        ON r.Fkvdfamily = pm.SourcePersonID
    INNER JOIN HospitalDW.dbo.DimPatient AS p
        ON p.PersonCode = pm.PatientMapKey
    INNER JOIN HospitalDW.dbo.DimService AS s
        ON s.ServiceName = 'Radiology'
    LEFT JOIN HospitalDW.dbo.DimPatientType AS pt
        ON pt.PatientTypeCode = r.PaientType
    WHERE r.deleted = 0
) AS src
"""

df = (
    spark.read.format("jdbc")
    .options(**connection)
    .option("dbtable", source_query)
    .load()
)

print("Extracted records:", df.count())


# Transform Jalali dates to Gregorian dates
@F.udf(returnType=DateType())
def jalali_to_gregorian(value):
    if value is None:
        return None

    try:
        parts = value.strip().replace("-", "/").split("/")
        if len(parts) != 3:
            return None

        y, m, d = map(int, parts)
        return jdatetime.date(y, m, d).togregorian()
    except (ValueError, TypeError, OverflowError):
        return None


transformed = (
    df.withColumn(
        "FullDate",
        jalali_to_gregorian(F.col("Turndate"))
    )
    .withColumn(
        "DateKey",
        F.date_format("FullDate", "yyyyMMdd").cast("int")
    )
    .withColumn(
        "TotalAmount",
        F.round("cash", 2).cast(DecimalType(18, 2))
    )
    .withColumn("SourceSystem", F.lit("Hospital"))
    .withColumn("SourceTable", F.lit("dbo.Radio_Recipe"))
)

# Validate transformation
print(
    "Invalid Gregorian dates:",
    transformed.filter(F.col("FullDate").isNull()).count()
)

print(
    "Missing total amounts:",
    transformed.filter(F.col("TotalAmount").isNull()).count()
)

print(
    "Unknown patient types:",
    transformed.filter(
        F.col("PaientType").isNotNull()
        & F.col("PatientTypeKey").isNull()
    ).count()
)

print(
    "Duplicate reception IDs:",
    transformed.groupBy("SourceVisitID")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

# Check DateKey values against DimDate
date_keys = (
    spark.read.format("jdbc")
    .options(**{
        **connection,
        "url": jdbc_url.replace(
            "databaseName=dayclinic",
            "databaseName=HospitalDW"
        )
    })
    .option("dbtable", "dbo.DimDate")
    .load()
    .select("DateKey")
)

missing_dates = (
    transformed.filter(F.col("DateKey").isNotNull())
    .join(date_keys, on="DateKey", how="left_anti")
    .count()
)

print("Dates missing from DimDate:", missing_dates)

#************************
# Select columns for the staging table
staging_df = transformed.select(
    "SourceSystem",
    "SourceTable",
    "SourceVisitID",
    "PatientKey",
    "DateKey",
    "ServiceKey",
    "PatientTypeKey",
    "ShiftCode",
    "PatientStatus",
    "TotalAmount"
)

# SQL Server Data Warehouse connection
dw_url = jdbc_url.replace(
    "databaseName=dayclinic",
    "databaseName=HospitalDW"
)

# Load data into the existing staging table
(
    staging_df.write
    .format("jdbc")
    .option("url", dw_url)
    .option("dbtable", "etl.StagingRadiology")
    .option("user", os.environ["SQL_USERNAME"])
    .option("password", os.environ["SQL_PASSWORD"])
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .mode("append")
    .save()
)

print("Staging load completed.")


spark.stop()
