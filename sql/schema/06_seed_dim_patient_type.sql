use HospitalDW;
Go

insert into DimPatientType 
(
 PatientTypeCode , 
 PatientTypeName

 ) 

select 
     s.PaientTypeCode ,
     s.PaientTypeName
from DayClinic.dbo.PaientType as s 
WHERE NOT EXISTS 
(
SELECT 1 
FROM dbo.DimPatientType as d 
where d.PatientTypeCode = s.PaientTypeCode
);