
import argparse
import os
import re
from pathlib import Path

import jdatetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType


# ============================================================
# 1. CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

HOSPITAL_DB = os.getenv("HOSPITAL_DB", "dayclinic")
DENTISTRY_DB = os.getenv("DENTISTRY_DB", "dentistry")
DW_DB = os.getenv("DW_DB", "HospitalDW")

JDBC_JAR = os.getenv(
    "MSSQL_JDBC_JAR",
    r"C:\SparkLab\jdbc\mssql-jdbc.jar"
)

SERVICES = {
    "radiology": {
        "table": "Radio_Recipe",
        "name": "Radiology",
        "system": "Hospital"
    },
    "sonography": {
        "table": "SONO_Recipe",
        "name": "Sonography",
        "system": "Hospital"
    },
    "physiotherapy": {
        "table": "Physio_Recipe",
        "name": "Physiotherapy",
        "system": "Hospital"
    },
    "emergency": {
        "table": "EMR_Recipe",
        "name": "Emergency",
        "system": "Hospital"
    },
    "psychology": {
        "table": "psychology_Recipe",
        "name": "Psychology",
        "system": "Hospital"
    },
    "procedures": {
        "table": "Dr_Procedures_Recipe",
        "name": "Procedures",
        "system": "Hospital"
    },
    "dentistry": {
        "table": "Recipe",
        "name": "Dentistry",
        "system": "Dental"
    }
}

STAGING_COLUMNS = [
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
]


# ============================================================
# 2. HELPERS
# ============================================================

def sql_identifier(value):
    """Permit simple SQL database identifiers only."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid database identifier: {value}")
    return f"[{value}]"


def create_spark():
    if not os.path.isfile(JDBC_JAR):
        raise FileNotFoundError(
            f"SQL Server JDBC driver not found: {JDBC_JAR}"
        )

    return (
        SparkSession.builder
        .appName("HospitalDW-InitialLoad")
        .master("local[4]")
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .config("spark.executor.extraClassPath", JDBC_JAR)
        .getOrCreate()
    )


def jdbc_connection():
    required = [
        "SQL_SERVER",
        "SQL_USERNAME",
        "SQL_PASSWORD"
    ]

    missing = [name for name in required if not os.getenv(name)]

    if missing:
        raise ValueError(
            "Missing .env settings: " + ", ".join(missing)
        )

    server = os.environ["SQL_SERVER"]
    port = os.getenv("SQL_PORT", "1433")

    url = (
        f"jdbc:sqlserver://{server}:{port};"
        f"databaseName={DW_DB};"
        "encrypt=true;"
        "trustServerCertificate=true"
    )

    return {
        "url": url,
        "user": os.environ["SQL_USERNAME"],
        "password": os.environ["SQL_PASSWORD"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    }


def read_jdbc(spark, connection, dbtable):
    return (
        spark.read
        .format("jdbc")
        .options(**connection)
        .option("dbtable", dbtable)
        .load()
    )


def read_query(spark, connection, sql):
    return read_jdbc(
        spark,
        connection,
        f"({sql}) AS src"
    )


def jalali_to_gregorian(value):
    if value is None:
        return None

    try:
        parts = str(value).strip().split("/")

        if len(parts) != 3:
            return None

        year, month, day = map(int, parts)

        return jdatetime.date(
            year, month, day
        ).togregorian()

    except (ValueError, TypeError, OverflowError):
        return None


jalali_date_udf = F.udf(
    jalali_to_gregorian,
    DateType()
)


# ============================================================
# 3. EXTRACT HOSPITAL SERVICE
# ============================================================

def extract_hospital(spark, connection, config):
    table = config["table"]

    source_query = f"""
        SELECT
            r.Turnid AS SourceVisitID,
            r.Fkvdfamily AS SourcePersonID,
            r.persno AS SourceFamilyID,
            r.Turndate AS JalaliDate,
            r.PaientType AS PatientTypeCode,
            r.Shiftcode AS ShiftCode,            
            r.cash AS TotalAmount
        FROM {sql_identifier(HOSPITAL_DB)}.dbo.[{table}] AS r
        WHERE r.deleted = 0
    """

    return read_query(spark, connection, source_query)


# ============================================================
# 4. EXTRACT AND CALCULATE DENTISTRY
# ============================================================

def extract_dentistry(spark, connection):
    db = sql_identifier(DENTISTRY_DB)

    # 4.1 Read receptions from 1393/04/01 onward
    recipe_df = read_query(
        spark,
        connection,
        f"""
        SELECT
            Turnid,
            Fkvdfamily,
            persno,
            Turndate,
            PaientType
        FROM {db}.dbo.Recipe
        WHERE Turndate >= N'1393/04/01'
          AND PaientType IN (1, 5)
        """
    ).alias("r")

    # 4.2 Read active dental services
    doctors_df = read_query(
        spark,
        connection,
        f"""
        SELECT
            recipeID,
            servicescode
        FROM {db}.dbo.doctorsService
        WHERE deleted = 0
        """
    ).alias("d")

    # 4.3 Read K values
    details_df = read_query(
        spark,
        connection,
        f"""
        SELECT
            Services_detail_Code,
            k_No
        FROM {db}.dbo.Services_detail
        """
    ).alias("s")

    # 4.4 Read annual coefficients
    tariff_df = read_query(
        spark,
        connection,
        f"""
        SELECT
            [year],
            Tariffkind,
            zarib
        FROM {db}.dbo.tariff
        WHERE Tariffkind IN (
            N'دولتی',
            N'بازنشستگی'
        )
        """
    )

    tariff_df = (
        tariff_df
        .withColumn(
            "TariffYear",
            F.trim(F.col("year").cast("string"))
        )
        .withColumn(
            "TariffCategory",
            F.trim(F.col("Tariffkind"))
        )
        .select(
            "TariffYear",
            "TariffCategory",
            "zarib"
        )
    )

    # Annual tariff must be unique per year/category
    duplicate_tariffs = (
        tariff_df
        .groupBy("TariffYear", "TariffCategory")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    if duplicate_tariffs > 0:
        raise ValueError(
            "Duplicate annual Dentistry tariffs found."
        )

    tariff_df = tariff_df.alias("t")

    # 4.5 Join receptions with performed services
    dental_services = (
        recipe_df
        .join(
            doctors_df,
            F.col("r.Turnid") == F.col("d.recipeID"),
            "inner"
        )
        .join(
            details_df,
            F.col("d.servicescode")
            == F.col("s.Services_detail_Code"),
            "left"
        )
    )

    # 4.6 Determine tariff category from patient type
    dental_services = dental_services.withColumn(
        "RequiredTariff",
        F.when(
            F.col("r.PaientType") == 1,
            F.lit("دولتی")
        ).when(
            F.col("r.PaientType") == 5,
            F.lit("بازنشستگی")
        )
    )

    # 4.7 Match the annual coefficient
    dental_services = dental_services.join(
        tariff_df,
        (
            F.substring(
                F.col("r.Turndate"), 1, 4
            ) == F.col("t.TariffYear")
        )
        &
        (
            F.col("RequiredTariff")
            == F.col("t.TariffCategory")
        ),
        "left"
    )

    # 4.8 Validate lookup completeness
    lookup_quality = dental_services.agg(
        F.sum(
            F.when(
                F.col("s.k_No").isNull(), 1
            ).otherwise(0)
        ).alias("MissingK"),

        F.sum(
            F.when(
                F.col("t.zarib").isNull(), 1
            ).otherwise(0)
        ).alias("MissingTariff")
    ).first()

    missing_k = lookup_quality["MissingK"] or 0
    missing_tariff = lookup_quality["MissingTariff"] or 0

    print("Dentistry services missing K:", missing_k)
    print(
        "Dentistry services missing tariff:",
        missing_tariff
    )

    if missing_k or missing_tariff:
        raise ValueError(
            "Dentistry pricing lookups are incomplete."
        )

    # 4.9 Calculate amount for each dental service
    dental_services = dental_services.select(
        F.col("r.Turnid").cast("long")
            .alias("SourceVisitID"),

        F.col("r.Fkvdfamily")
            .alias("SourcePersonID"),

        F.col("r.persno")
            .alias("SourceFamilyID"),

        F.col("r.Turndate")
            .alias("JalaliDate"),

        F.col("r.PaientType")
            .alias("PatientTypeCode"),

        (
            F.col("s.k_No").cast("decimal(18,4)")
            *
            F.col("t.zarib").cast("decimal(18,4)")
        ).alias("ServiceAmount")
    )

    # 4.10 Aggregate amounts by reception
    dentistry_amounts = (
        dental_services
        .groupBy(
            "SourceVisitID",
            "SourcePersonID",
            "SourceFamilyID",
            "JalaliDate",
            "PatientTypeCode"
        )
        .agg(
            F.sum("ServiceAmount")
            .cast("decimal(18,2)")
            .alias("TotalAmount")
        )
    )

    # The provided Recipe schema did not include
    # ShiftCode or PatientStatus.
    dentistry_amounts = (
        dentistry_amounts
        .withColumn(
            "ShiftCode",
            F.lit(None).cast("int")
        )
        .withColumn(
            "PatientStatus",
            F.lit(None).cast("int")
        )
    )

    return dentistry_amounts


# ============================================================
# 5. LOAD DIMENSION LOOKUPS
# ============================================================

def load_dimensions(spark, connection):
    patient_map = read_jdbc(
        spark, connection, "etl.PatientMap"
    ).select(
        "SourcePersonID",
        "PatientMapKey"
    )

    family_map = read_jdbc(
        spark, connection, "etl.FamilyMap"
    ).select(
        "SourceFamilyID",
        "FamilyMapKey"
    )

    patients = read_jdbc(
        spark, connection, "dbo.DimPatient"
    ).select(
        "PatientKey",
        "PersonCode",
        "FamilyCode"
    )

    dates = read_jdbc(
        spark, connection, "dbo.DimDate"
    ).select("DateKey")

    patient_types = read_jdbc(
        spark, connection, "dbo.DimPatientType"
    ).select(
        "PatientTypeKey",
        "PatientTypeCode"
    )

    services = read_jdbc(
        spark, connection, "dbo.DimService"
    ).select(
        "ServiceKey",
        "ServiceName"
    )

    return {
        "patient_map": patient_map,
        "family_map": family_map,
        "patients": patients,
        "dates": dates,
        "patient_types": patient_types,
        "services": services
    }


# ============================================================
# 6. TRANSFORM AND MAP DIMENSIONS
# ============================================================

def transform_source(source_df, config, dims):
    service_matches = (
        dims["services"]
        .filter(
            F.lower(F.trim(F.col("ServiceName")))
            == config["name"].lower()
        )
        .select("ServiceKey")
        .collect()
    )

    if len(service_matches) != 1:
        raise ValueError(
            "Expected exactly one DimService entry for "
            + config["name"]
        )

    service_key = service_matches[0]["ServiceKey"]

    # 6.1 Normalize source column types
    base = (
        source_df
        .withColumn(
            "SourceVisitID",
            F.col("SourceVisitID").cast("long")
        )
        .withColumn(
            "SourcePersonID",
            F.col("SourcePersonID").cast("int")
        )
        .withColumn(
            "SourceFamilyID",
            F.col("SourceFamilyID").cast("int")
        )
        .withColumn(
            "PatientTypeCode",
            F.col("PatientTypeCode").cast("int")
        )
        .withColumn(
            "TotalAmount",
            F.col("TotalAmount").cast("decimal(18,2)")
        )
        .withColumn(
            "ShiftCode",
            F.col("ShiftCode").cast("int")
        )      
    )

    # 6.2 Convert Jalali date to Gregorian
    base = base.withColumn(
        "GregorianDate",
        jalali_date_udf(F.col("JalaliDate"))
    )

    # 6.3 Generate YYYYMMDD DateKey
    base = base.withColumn(
        "DateKey",
        F.date_format(
            F.col("GregorianDate"), "yyyyMMdd"
        ).cast("int")
    )

    # 6.4 Join patient/family mapping and dimensions
    mapped = (
        base.alias("b")
        .join(
            dims["patient_map"].alias("pm"),
            F.col("b.SourcePersonID")
            == F.col("pm.SourcePersonID"),
            "left"
        )
        .join(
            dims["family_map"].alias("fm"),
            F.col("b.SourceFamilyID")
            == F.col("fm.SourceFamilyID"),
            "left"
        )
        .join(
            dims["patients"].alias("dp"),
            (
                F.col("pm.PatientMapKey")
                == F.col("dp.PersonCode")
            )
            &
            (
                F.col("fm.FamilyMapKey")
                == F.col("dp.FamilyCode")
            ),
            "left"
        )
        .join(
            dims["patient_types"].alias("pt"),
            F.col("b.PatientTypeCode")
            == F.col("pt.PatientTypeCode"),
            "left"
        )
        .join(
            dims["dates"].alias("dd"),
            F.col("b.DateKey")
            == F.col("dd.DateKey"),
            "left"
        )
    )

    # Temporary mapping diagnostics
    eligible = mapped.filter(
      F.col("b.TotalAmount").isNotNull()
      & (F.col("b.TotalAmount") != 0)
    )
    mapping_status = eligible.withColumn(
      "MappingStatus",
      F.when(
         F.col("pm.PatientMapKey").isNull(),
        "Missing PatientMap"
      )
      .when(
        F.col("fm.FamilyMapKey").isNull(),
        "Missing FamilyMap"
      )
      .when(
        F.col("dp.PatientKey").isNull(),
        "Missing DimPatient"
     )
     .otherwise("Mapped")
    )

    mapping_status.groupBy("MappingStatus") \
      .count() \
      .show(truncate=False)

    eligible.agg(
      F.count("*").alias("EligibleRows"),

      F.sum(
        F.when(
            F.col("pm.PatientMapKey").isNull(), 1
        ).otherwise(0)
      ).alias("MissingPatientMap"),

      F.sum(
        F.when(
            F.col("fm.FamilyMapKey").isNull(), 1
        ).otherwise(0)
      ).alias("MissingFamilyMap"),

      F.sum(
        F.when(
            F.col("pm.PatientMapKey").isNotNull()
            & F.col("fm.FamilyMapKey").isNotNull()
            & F.col("dp.PatientKey").isNull(),
            1
        ).otherwise(0)
      ).alias("MissingDimPatientMatch"),

      F.sum(
        F.when(
            F.col("dp.PatientKey").isNull(), 1
        ).otherwise(0)
      ).alias("TotalUnmappedEligible")
    ).show(truncate=False)

    print("=== MAPPING DIAGNOSTICS CODE IS RUNNING ===", flush=True)

    # Temporary diagnostic: determine why DimPatient did not match.
    # This only inspects rows with valid, nonzero amounts and both mappings.
    print("=== CHECKING MISSING DIM PATIENT ===", flush=True)

    missing_dim = (
        eligible
        .filter(
            F.col("pm.PatientMapKey").isNotNull()
            & F.col("fm.FamilyMapKey").isNotNull()
            & F.col("dp.PatientKey").isNull()
        )
        .select(
            F.col("pm.PatientMapKey").alias("MappedPersonCode"),
            F.col("fm.FamilyMapKey").alias("MappedFamilyCode")
        )
    )

    persons = (
        dims["patients"]
        .select(F.col("PersonCode").alias("ExistingPersonCode"))
        .distinct()
    )

    families = (
        dims["patients"]
        .select(F.col("FamilyCode").alias("ExistingFamilyCode"))
        .distinct()
    )

    check = (
        missing_dim
        .join(
            persons,
            F.col("MappedPersonCode") == F.col("ExistingPersonCode"),
            "left"
        )
        .join(
            families,
            F.col("MappedFamilyCode") == F.col("ExistingFamilyCode"),
            "left"
        )
    )

    check.agg(
        F.count("*").alias("MissingDimPatientRows"),
        F.sum(
            F.when(F.col("ExistingPersonCode").isNull(), 1)
            .otherwise(0)
        ).alias("PersonCodeNotFound"),
        F.sum(
            F.when(F.col("ExistingFamilyCode").isNull(), 1)
            .otherwise(0)
        ).alias("FamilyCodeNotFound"),
        F.sum(
            F.when(
                F.col("ExistingPersonCode").isNotNull()
                & F.col("ExistingFamilyCode").isNotNull(),
                1
            ).otherwise(0)
        ).alias("BothCodesExistButPairMissing")
    ).show(truncate=False)

    # 6.5 Prepare unified warehouse schema
    transformed = mapped.select(
        F.lit(config["system"]).alias("SourceSystem"),

        F.lit(
            "dbo." + config["table"]
        ).alias("SourceTable"),

        F.col("b.SourceVisitID"),

        F.col("dp.PatientKey")
            .alias("PatientKey"),

        F.col("b.DateKey"),

        F.lit(service_key)
            .cast("int")
            .alias("ServiceKey"),

        F.col("pt.PatientTypeKey")
            .alias("PatientTypeKey"),

        F.col("b.ShiftCode"),
        F.col("b.PatientStatus"),
        F.col("b.TotalAmount"),
        F.col("b.GregorianDate"),

        # Used to validate DimDate matching
        F.col("dd.DateKey").alias("MatchedDateKey")
    )

    return transformed


# ============================================================
# 7. VALIDATION
# ============================================================

def validate_and_filter(transformed):
    transformed = transformed.cache()

    metrics = transformed.agg(
        F.count("*").alias("Total"),

        F.sum(
            F.when(
                F.col("GregorianDate").isNull(), 1
            ).otherwise(0)
        ).alias("InvalidDates"),

        F.sum(
            F.when(
                F.col("TotalAmount").isNull(), 1
            ).otherwise(0)
        ).alias("NullAmounts"),

        F.sum(
            F.when(
                F.col("TotalAmount") == 0, 1
            ).otherwise(0)
        ).alias("ZeroAmounts"),

        F.sum(
            F.when(
                F.col("PatientTypeKey").isNull(), 1
            ).otherwise(0)
        ).alias("UnknownTypes"),

        F.sum(
            F.when(
                F.col("PatientKey").isNull(), 1
            ).otherwise(0)
        ).alias("UnmappedPatients"),

        F.sum(
            F.when(
                F.col("GregorianDate").isNotNull()
                & F.col("MatchedDateKey").isNull(),
                1
            ).otherwise(0)
        ).alias("MissingDates")
    ).first()

    duplicates = (
        transformed
        .groupBy("SourceVisitID")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    total = metrics["Total"]
    null_amounts = metrics["NullAmounts"] or 0
    zero_amounts = metrics["ZeroAmounts"] or 0
    invalid_dates = metrics["InvalidDates"] or 0
    unknown_types = metrics["UnknownTypes"] or 0
    missing_dates = metrics["MissingDates"] or 0
    unmapped = metrics["UnmappedPatients"] or 0

    print("Total transformed:", total)
    print("Invalid Gregorian dates:", invalid_dates)
    print("Missing total amounts:", null_amounts)
    print("Zero total amounts:", zero_amounts)
    print("Unknown patient types:", unknown_types)
    print("Duplicate reception IDs:", duplicates)
    print("Dates missing from DimDate:", missing_dates)
    print("Unmapped patients:", unmapped)

    # Fail on problems that require investigation
    if (
        invalid_dates > 0
        or unknown_types > 0
        or missing_dates > 0
        or duplicates > 0
    ):
        raise ValueError(
            "Validation failed. No data should be loaded."
        )

    # Keep negative amounts, exclude NULL and zero.
    # Also exclude records without a valid PatientKey.
    valid_df = transformed.filter(
        F.col("TotalAmount").isNotNull()
        & (F.col("TotalAmount") != 0)
        & F.col("PatientKey").isNotNull()
        & F.col("DateKey").isNotNull()
        & F.col("PatientTypeKey").isNotNull()
    )

    staging_df = valid_df.select(*STAGING_COLUMNS)

    print(
        "Records excluded due to invalid amount:",
        null_amounts + zero_amounts
    )
    print(
        "Records ready for staging:",
        staging_df.count()
    )

    return staging_df


# ============================================================
# 8. STAGING LOAD
# ============================================================

def load_staging(
    spark, connection, staging_df, config
):
    source_system = config["system"]
    source_table = "dbo." + config["table"]

    # Prevent accidental duplicate initial loads
    existing_sql = f"""
        SELECT COUNT_BIG(*) AS ExistingRows
        FROM etl.StagingService
        WHERE SourceSystem = '{source_system}'
          AND SourceTable = '{source_table}'
    """

    existing_rows = read_query(
        spark, connection, existing_sql
    ).first()["ExistingRows"]

    if existing_rows > 0:
        raise ValueError(
            f"{existing_rows} records already exist "
            f"for {source_table} in StagingService. "
            "Initial load cancelled."
        )

    # Final safety validation
    invalid_rows = staging_df.filter(
        F.col("TotalAmount").isNull()
        | (F.col("TotalAmount") == 0)
    ).count()

    if invalid_rows > 0:
        raise ValueError(
            "Invalid amounts found before staging load."
        )

    (
        staging_df.write
        .format("jdbc")
        .options(**connection)
        .option("dbtable", "etl.StagingService")
        .mode("append")
        .save()
    )

    print("Staging load completed.")


# ============================================================
# 9. MAIN PIPELINE
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--service",
        required=True,
        choices=list(SERVICES.keys())
    )

    parser.add_argument(
        "--validate-only",
        action="store_true"
    )

    args = parser.parse_args()
    config = SERVICES[args.service]

    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        connection = jdbc_connection()

        print("=" * 55)
        print("Service:", args.service)
        print("Validate only:", args.validate_only)
        print("=" * 55)

        # STEP 1: Extract and calculate amounts
        if args.service == "dentistry":
            df = extract_dentistry(
                spark, connection
            )
        else:
            df = extract_hospital(
                spark, connection, config
            )

        # STEP 2: Load warehouse dimensions
        dims = load_dimensions(spark, connection)

        # STEP 3: Transform to warehouse schema
        transformed = transform_source(
            df, config, dims
        )

        unmapped_eligible = transformed.filter(
         F.col("PatientKey").isNull()
         & F.col("TotalAmount").isNotNull()
         & (F.col("TotalAmount") != 0)
        )
 
        print(
           "Unmapped eligible Dentistry visits:",
           unmapped_eligible.count()
        )
        

        # STEP 4: Validate and filter
        staging_df = validate_and_filter(
            transformed
        )

        # STEP 5: Write only when not validating
        if args.validate_only:
            print(
                "Validation completed. "
                "No records inserted."
            )
        else:
            load_staging(
                spark,
                connection,
                staging_df,
                config
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
