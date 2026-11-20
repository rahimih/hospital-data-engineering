USE HospitalDW;
GO

INSERT INTO dbo.DimService (ServiceName)
VALUES
    ('Radiology'),
    ('Sonography'),
    ('Physiotherapy'),
    ('Emergency'),
    ('Procedures'),
    ('psychology'),
    ('Dentistry');
GO