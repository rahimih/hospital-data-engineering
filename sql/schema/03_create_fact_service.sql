
USE HospitalDW;
GO

CREATE TABLE dbo.FactService
(
    ServiceFactKey BIGINT IDENTITY(1,1) NOT NULL
        PRIMARY KEY,

    -- Dimension foreign keys
    PatientKey INT NOT NULL,
    DateKey INT NOT NULL,
    ServiceKey INT NOT NULL,
    PatientTypeKey INT NULL,

    -- Source identifiers
    SourceSystem VARCHAR(30) NOT NULL,
    SourceTable VARCHAR(100) NOT NULL,
    SourceVisitID BIGINT NOT NULL,

    -- Reception attributes
    ShiftCode TINYINT NULL,   

    -- Financial measure: source cash
    TotalAmount DECIMAL(18,2) NULL,

  
    -- ETL audit
    LoadedAt DATETIME2(0) NOT NULL
        DEFAULT (SYSUTCDATETIME()),

    -- Foreign keys
    CONSTRAINT FK_FactService_Patient
        FOREIGN KEY (PatientKey)
        REFERENCES dbo.DimPatient(PatientKey),

    CONSTRAINT FK_FactService_Date
        FOREIGN KEY (DateKey)
        REFERENCES dbo.DimDate(DateKey),

    CONSTRAINT FK_FactService_Service
        FOREIGN KEY (ServiceKey)
        REFERENCES dbo.DimService(ServiceKey),

    CONSTRAINT FK_FactService_PatientType
        FOREIGN KEY (PatientTypeKey)
        REFERENCES dbo.DimPatientType(PatientTypeKey),

    -- Prevent duplicate source receptions
    CONSTRAINT UQ_FactService_SourceVisit
        UNIQUE (
            SourceSystem,
            SourceTable,
            SourceVisitID
        ),

   
);
go