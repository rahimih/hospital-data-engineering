
# Available date range in the warehouse
DATE_RANGE = """
SELECT
    MIN(d.FullDate) AS MinDate,
    MAX(d.FullDate) AS MaxDate
FROM dbo.FactService AS f
JOIN dbo.DimDate AS d
    ON f.DateKey = d.DateKey
"""


SERVICES = """
SELECT DISTINCT s.ServiceName
FROM dbo.FactService AS f
JOIN dbo.DimService AS s
    ON f.ServiceKey = s.ServiceKey
ORDER BY s.ServiceName
"""


PATIENT_TYPES = """
SELECT DISTINCT
    COALESCE(pt.PatientTypeName, 'Unknown') AS PatientType
FROM dbo.FactService AS f
LEFT JOIN dbo.DimPatientType AS pt
    ON f.PatientTypeKey = pt.PatientTypeKey
ORDER BY PatientType
"""


SOURCES = """
SELECT DISTINCT SourceSystem
FROM dbo.FactService
ORDER BY SourceSystem
"""


# Shared filters for reports
BASE_FILTER = """
FROM dbo.FactService AS f

JOIN dbo.DimDate AS d
    ON f.DateKey = d.DateKey

JOIN dbo.DimService AS s
    ON f.ServiceKey = s.ServiceKey

LEFT JOIN dbo.DimPatientType AS pt
    ON f.PatientTypeKey = pt.PatientTypeKey

WHERE d.FullDate >= :start_date
  AND d.FullDate < DATEADD(
      day, 1, CAST(:end_date AS date)
  )
  AND (
      :service = 'All'
      OR s.ServiceName = :service
  )
  AND (
      :patient_type = 'All'
      OR COALESCE(
          pt.PatientTypeName, 'Unknown'
      ) = :patient_type
  )
  AND (
      :source = 'All'
      OR f.SourceSystem = :source
  )
"""


# 1. Dashboard KPI summary
SUMMARY = """
SELECT
    COUNT_BIG(*) AS TotalVisits,

    COUNT(DISTINCT f.PatientKey) AS UniquePatients,

    SUM(
        CASE
            WHEN f.SourceSystem = 'Hospital'
            THEN COALESCE(f.TotalAmount, 0)
            ELSE 0
        END
    ) AS HospitalCash,

    SUM(
        CASE
            WHEN f.SourceSystem = 'Dental'
            THEN COALESCE(f.TotalAmount, 0)
            ELSE 0
        END
    ) AS DentistryTariffAmount

""" + BASE_FILTER


# 2. Daily visits and amounts
DAILY = """
SELECT
    CAST(d.FullDate AS date) AS VisitDate,

    COUNT_BIG(*) AS TotalVisits,

    COUNT(DISTINCT f.PatientKey) AS UniquePatients,

    SUM(
        CASE
            WHEN f.SourceSystem = 'Hospital'
            THEN COALESCE(f.TotalAmount, 0)
            ELSE 0
        END
    ) AS HospitalCash,

    SUM(
        CASE
            WHEN f.SourceSystem = 'Dental'
            THEN COALESCE(f.TotalAmount, 0)
            ELSE 0
        END
    ) AS DentistryTariffAmount

""" + BASE_FILTER + """
GROUP BY CAST(d.FullDate AS date)
ORDER BY VisitDate
"""


# 3. Monthly visits and patient counts
MONTHLY = """
SELECT
    DATEFROMPARTS(
        YEAR(d.FullDate),
        MONTH(d.FullDate),
        1
    ) AS MonthStart,

    COUNT_BIG(*) AS TotalVisits,

    COUNT(DISTINCT f.PatientKey) AS UniquePatients

""" + BASE_FILTER + """
GROUP BY
    YEAR(d.FullDate),
    MONTH(d.FullDate)

ORDER BY MonthStart
"""


# 4. Rank services by visits and patients
SERVICE_RANKING = """
SELECT
    s.ServiceName,

    COUNT_BIG(*) AS TotalVisits,

    COUNT(DISTINCT f.PatientKey) AS UniquePatients

""" + BASE_FILTER + """
GROUP BY s.ServiceName
ORDER BY TotalVisits DESC
"""


# 5. Visits grouped by patient type
PATIENT_TYPE_REPORT = """
SELECT
    COALESCE(
        pt.PatientTypeName, 'Unknown'
    ) AS PatientType,

    COUNT_BIG(*) AS TotalVisits,

    COUNT(DISTINCT f.PatientKey) AS UniquePatients

""" + BASE_FILTER + """
GROUP BY
    COALESCE(pt.PatientTypeName, 'Unknown')

ORDER BY TotalVisits DESC
"""


# 6. Monthly trends grouped by patient type
MONTHLY_PATIENT_TYPES = """
SELECT
    DATEFROMPARTS(
        YEAR(d.FullDate),
        MONTH(d.FullDate),
        1
    ) AS MonthStart,

    COALESCE(
        pt.PatientTypeName, 'Unknown'
    ) AS PatientType,

    COUNT_BIG(*) AS TotalVisits

""" + BASE_FILTER + """
GROUP BY
    YEAR(d.FullDate),
    MONTH(d.FullDate),
    COALESCE(pt.PatientTypeName, 'Unknown')

ORDER BY MonthStart
"""


# 7. Emergency cash by day
# Always uses Hospital + Emergency, regardless
# of the selected source/service filters.
EMERGENCY_DAILY = """
SELECT
    CAST(d.FullDate AS date) AS VisitDate,

    COUNT_BIG(*) AS TotalVisits,

    SUM(
        COALESCE(f.TotalAmount, 0)
    ) AS EmergencyCash

FROM dbo.FactService AS f

JOIN dbo.DimDate AS d
    ON f.DateKey = d.DateKey

JOIN dbo.DimService AS s
    ON f.ServiceKey = s.ServiceKey

LEFT JOIN dbo.DimPatientType AS pt
    ON f.PatientTypeKey = pt.PatientTypeKey

WHERE f.SourceSystem = 'Hospital'
  AND s.ServiceName = 'Emergency'
  AND d.FullDate >= :start_date
  AND d.FullDate < DATEADD(
      day, 1, CAST(:end_date AS date)
  )
  AND (
      :patient_type = 'All'
      OR COALESCE(
          pt.PatientTypeName, 'Unknown'
      ) = :patient_type
  )

GROUP BY CAST(d.FullDate AS date)
ORDER BY VisitDate
"""
