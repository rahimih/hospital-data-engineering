USE HospitalDW;
GO

DECLARE @StartDate DATE = '2014-01-01';
DECLARE @EndDate DATE = '2030-12-31';

;WITH CalendarDates AS
(
    SELECT @StartDate AS CalendarDate

    UNION ALL

    SELECT DATEADD(DAY, 1, CalendarDate)
    FROM CalendarDates
    WHERE CalendarDate < @EndDate
)
INSERT INTO dbo.DimDate
(
    DateKey,
    FullDate,
    [Year],
    [Quarter],
    [Month],
    MonthName,
    [Day],
    DayOfWeek
)
SELECT
    CONVERT(INT, CONVERT(CHAR(8), CalendarDate, 112)),
    CalendarDate,
    YEAR(CalendarDate),
    DATEPART(QUARTER, CalendarDate),
    MONTH(CalendarDate),

    CHOOSE(
        MONTH(CalendarDate),
        'January', 'February', 'March',
        'April', 'May', 'June',
        'July', 'August', 'September',
        'October', 'November', 'December'
    ),

    DAY(CalendarDate),

    -- Monday = 1, Sunday = 7
    (DATEDIFF(DAY, '19000101', CalendarDate) % 7) + 1

FROM CalendarDates AS c
WHERE NOT EXISTS
(
    SELECT 1
    FROM dbo.DimDate AS d
    WHERE d.FullDate = c.CalendarDate
)
OPTION (MAXRECURSION 0);
GO