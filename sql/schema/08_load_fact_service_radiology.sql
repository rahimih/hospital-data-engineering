
USE HospitalDW;
GO

SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    INSERT INTO dbo.FactService
    (
        PatientKey,
        DateKey,
        ServiceKey,
        PatientTypeKey,
        SourceSystem,
        SourceTable,
        SourceVisitID,
        ShiftCode,
        PatientStatus,
        TotalAmount
    )
    SELECT
        s.PatientKey,
        s.DateKey,
        s.ServiceKey,
        s.PatientTypeKey,
        s.SourceSystem,
        s.SourceTable,
        s.SourceVisitID,
        s.ShiftCode,
        s.PatientStatus,
        s.TotalAmount
    FROM etl.StagingRadiology AS s
    WHERE NOT EXISTS
    (
        SELECT 1
        FROM dbo.FactService AS f WITH (UPDLOCK, HOLDLOCK)
        WHERE f.SourceSystem = s.SourceSystem
          AND f.SourceTable = s.SourceTable
          AND f.SourceVisitID = s.SourceVisitID
    );

    DECLARE @InsertedRows INT = @@ROWCOUNT;

    COMMIT TRANSACTION;

    SELECT @InsertedRows AS InsertedRows;

END TRY

BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
GO
