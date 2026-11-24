
USE HospitalDW;
GO

SET NOCOUNT ON;
SET XACT_ABORT ON;

-- 1. Collect patients from all source tables
SELECT
    Fkvdfamily AS SourcePersonID,
    persno AS SourceFamilyID
INTO #AllPatients
FROM
(
    SELECT Fkvdfamily, persno FROM dayclinic.dbo.Radio_Recipe
    UNION ALL
    SELECT Fkvdfamily, persno FROM dayclinic.dbo.EMR_Recipe
    UNION ALL
    SELECT Fkvdfamily, persno FROM dayclinic.dbo.SONO_Recipe
    UNION ALL
    SELECT Fkvdfamily, persno FROM dayclinic.dbo.Physio_Recipe
    UNION ALL
    SELECT Fkvdfamily, persno FROM dayclinic.dbo.psychology_Recipe
    UNION ALL
    SELECT Fkvdfamily, persno FROM dayclinic.dbo.Dr_Procedures_Recipe
    UNION ALL
    SELECT Fkvdfamily, persno FROM dentistry.dbo.Recipe
) AS SourceData;


-- 2. Identify valid patients
SELECT
    SourcePersonID,
    MIN(SourceFamilyID) AS SourceFamilyID,
    COUNT(DISTINCT SourceFamilyID) AS FamilyCount,
    MAX(
        CASE WHEN SourceFamilyID IS NULL
             THEN 1 ELSE 0 END
    ) AS HasNullFamily
INTO #PatientQuality
FROM #AllPatients
WHERE SourcePersonID IS NOT NULL
GROUP BY SourcePersonID;


SELECT SourcePersonID, SourceFamilyID
INTO #ValidPatients
FROM #PatientQuality
WHERE FamilyCount = 1
  AND HasNullFamily = 0;


SELECT SourcePersonID
INTO #InvalidPatients
FROM #PatientQuality
WHERE FamilyCount <> 1
   OR HasNullFamily = 1;


-- 3. Preview counts
SELECT
    (SELECT COUNT(*) FROM #ValidPatients) AS ValidPatients,
    (SELECT COUNT(*) FROM #InvalidPatients) AS InvalidPatients,
    (SELECT COUNT(*) FROM dbo.DimPatient) AS ExistingPatients;


-- 4. Apply changes atomically
BEGIN TRY
    BEGIN TRANSACTION;

    -- Safety: do not delete patients referenced by facts
    IF EXISTS
    (
        SELECT 1
        FROM dbo.FactService AS f
        JOIN dbo.DimPatient AS d
            ON f.PatientKey = d.PatientKey
        JOIN etl.PatientMap AS pm
            ON d.PersonCode = pm.PatientMapKey
        JOIN #InvalidPatients AS i
            ON pm.SourcePersonID = i.SourcePersonID
    )
        THROW 50001,
            'Invalid patients have fact records. Review before loading.',
            1;


    -- Exclude previously loaded invalid patients
    DELETE d
    FROM dbo.DimPatient AS d
    JOIN etl.PatientMap AS pm
        ON d.PersonCode = pm.PatientMapKey
    JOIN #InvalidPatients AS i
        ON pm.SourcePersonID = i.SourcePersonID;


    -- Add missing family mappings
    INSERT INTO etl.FamilyMap (SourceFamilyID)
    SELECT DISTINCT v.SourceFamilyID
    FROM #ValidPatients AS v
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM etl.FamilyMap AS fm
        WHERE fm.SourceFamilyID = v.SourceFamilyID
    );


    -- Check existing patient-to-family mappings
    IF EXISTS
    (
        SELECT 1
        FROM #ValidPatients AS v
        JOIN etl.PatientMap AS pm
            ON pm.SourcePersonID = v.SourcePersonID
        JOIN etl.FamilyMap AS fm
            ON fm.FamilyMapKey = pm.FamilyMapKey
        WHERE fm.SourceFamilyID <> v.SourceFamilyID
    )
        THROW 50002,
            'Existing patient-family mapping conflict.',
            1;


    -- Add missing patient mappings
    INSERT INTO etl.PatientMap
    (
        SourcePersonID,
        FamilyMapKey
    )
    SELECT
        v.SourcePersonID,
        fm.FamilyMapKey
    FROM #ValidPatients AS v
    JOIN etl.FamilyMap AS fm
        ON fm.SourceFamilyID = v.SourceFamilyID
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM etl.PatientMap AS pm
        WHERE pm.SourcePersonID = v.SourcePersonID
    );


    -- Add missing DimPatient records
    INSERT INTO dbo.DimPatient
    (
        PersonCode,
        FamilyCode
    )
    SELECT
        pm.PatientMapKey,
        fm.FamilyMapKey
    FROM #ValidPatients AS v
    JOIN etl.PatientMap AS pm
        ON pm.SourcePersonID = v.SourcePersonID
    JOIN etl.FamilyMap AS fm
        ON fm.SourceFamilyID = v.SourceFamilyID
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dbo.DimPatient AS d
        WHERE d.PersonCode = pm.PatientMapKey
    );


    -- Verify all valid patients have correct mappings
    IF EXISTS
    (
        SELECT 1
        FROM #ValidPatients AS v
        JOIN etl.PatientMap AS pm
            ON pm.SourcePersonID = v.SourcePersonID
        JOIN etl.FamilyMap AS fm
            ON fm.SourceFamilyID = v.SourceFamilyID
        LEFT JOIN dbo.DimPatient AS d
            ON d.PersonCode = pm.PatientMapKey
        WHERE d.PatientKey IS NULL
           OR d.FamilyCode <> fm.FamilyMapKey
    )
        THROW 50003,
            'DimPatient mapping validation failed.',
            1;

    COMMIT TRANSACTION;

    PRINT 'DimPatient initial load completed.';
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;


-- 5. Final verification
SELECT
    COUNT(*) AS TotalPatients,
    COUNT(DISTINCT FamilyCode) AS TotalFamilies
FROM dbo.DimPatient;

DROP TABLE #InvalidPatients;
DROP TABLE #ValidPatients;
DROP TABLE #PatientQuality;
DROP TABLE #AllPatients;
