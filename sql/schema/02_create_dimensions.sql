USE HospitalDW;
GO

CREATE TABLE dbo.DimPatient
(
    PatientKey INT IDENTITY(1,1) NOT NULL
        PRIMARY KEY,

    PersonCode int NOT NULL,

    FamilyCode int NOT NULL,

    CONSTRAINT UQ_DimPatient_PersonCode
        UNIQUE (PersonCode)
);



CREATE TABLE dbo.DimDate
(
    DateKey INT NOT NULL PRIMARY KEY,

    FullDate DATE NOT NULL,

    [Year] SMALLINT NOT NULL,

    [Quarter] TINYINT NOT NULL,

    [Month] TINYINT NOT NULL,

    MonthName VARCHAR(20) NOT NULL,

    [Day] TINYINT NOT NULL,

    DayOfWeek TINYINT NOT NULL,

    CONSTRAINT UQ_DimDate_FullDate
        UNIQUE (FullDate)
);


CREATE TABLE dbo.DimService
(
    ServiceKey INT IDENTITY(1,1) NOT NULL
        PRIMARY KEY,

    ServiceName VARCHAR(50) NOT NULL,

    CONSTRAINT UQ_DimService_Name
        UNIQUE (ServiceName)
);



CREATE TABLE dbo.DimPatientType
(
    PatientTypeKey INT IDENTITY(1,1) NOT NULL
        PRIMARY KEY,

    PatientTypeCode TINYINT NOT NULL,

    PatientTypeName NVARCHAR(50) NOT NULL,

    CONSTRAINT UQ_DimPatientType_Code
        UNIQUE (PatientTypeCode)
);






GO