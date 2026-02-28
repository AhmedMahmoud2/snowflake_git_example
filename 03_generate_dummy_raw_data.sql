-- 03_generate_dummy_raw_data.sql
-- Generates synthetic A&E attendances and intentionally injects data quality issues.

USE DATABASE NHS_DEMO;
USE SCHEMA AE;

TRUNCATE TABLE AE_ATTENDANCE_RAW;

-- Generate 200 rows
INSERT INTO AE_ATTENDANCE_RAW (
  ATTENDANCE_ID, NHS_NUMBER, HOSPITAL_SITE_CODE, TRUST_CODE,
  PATIENT_DOB, PATIENT_AGE, PATIENT_GENDER, PATIENT_POSTCODE,
  ARRIVAL_TS, TRIAGE_TS, FIRST_CLINICIAN_TS, DISCHARGE_TS,
  ARRIVAL_MODE, CHIEF_COMPLAINT, DIAGNOSIS_CODE, DIAGNOSIS_DESC,
  WAIT_MINS_TO_TRIAGE, WAIT_MINS_TO_CLINICIAN, TOTAL_LOS_MINS,
  FOUR_HOUR_BREACH_FLAG
)
WITH base AS (
  SELECT
    SEQ4() AS RN,
    DATEADD('day', -UNIFORM(0, 60, RANDOM()), CURRENT_DATE()) AS ARRIVAL_DATE,
    UNIFORM(0, 23, RANDOM()) AS ARRIVAL_HOUR,
    UNIFORM(0, 59, RANDOM()) AS ARRIVAL_MIN
  FROM TABLE(GENERATOR(ROWCOUNT => 200))
),
ts AS (
  SELECT
    RN,
    TO_TIMESTAMP_NTZ(TO_VARCHAR(ARRIVAL_DATE) || ' ' ||
      LPAD(ARRIVAL_HOUR::STRING,2,'0') || ':' || LPAD(ARRIVAL_MIN::STRING,2,'0') || ':00') AS ARRIVAL_TS,
    DATEADD('minute', UNIFORM(0, 45, RANDOM()), 
      TO_TIMESTAMP_NTZ(TO_VARCHAR(ARRIVAL_DATE) || ' ' ||
      LPAD(ARRIVAL_HOUR::STRING,2,'0') || ':' || LPAD(ARRIVAL_MIN::STRING,2,'0') || ':00')) AS TRIAGE_TS,
    DATEADD('minute', UNIFORM(10, 240, RANDOM()),
      TO_TIMESTAMP_NTZ(TO_VARCHAR(ARRIVAL_DATE) || ' ' ||
      LPAD(ARRIVAL_HOUR::STRING,2,'0') || ':' || LPAD(ARRIVAL_MIN::STRING,2,'0') || ':00')) AS FIRST_CLINICIAN_TS,
    DATEADD('minute', UNIFORM(30, 600, RANDOM()),
      TO_TIMESTAMP_NTZ(TO_VARCHAR(ARRIVAL_DATE) || ' ' ||
      LPAD(ARRIVAL_HOUR::STRING,2,'0') || ':' || LPAD(ARRIVAL_MIN::STRING,2,'0') || ':00')) AS DISCHARGE_TS
  FROM base
),
patients AS (
  SELECT
    RN,
    -- a pseudo NHS number (not valid; synthetic)
    LPAD(UNIFORM(100000000, 999999999, RANDOM())::STRING, 10, '0') AS NHS_NUMBER_OK,
    DATEADD('year', -UNIFORM(0, 95, RANDOM()), DATEADD('day', -UNIFORM(0, 364, RANDOM()), CURRENT_DATE())) AS DOB_OK,
    UNIFORM(0, 95, RANDOM()) AS AGE_OK,
    IFF(UNIFORM(0, 10, RANDOM()) < 5, 'F', 'M') AS GENDER_OK,
    -- a rough postcode-like pattern
    IFF(UNIFORM(0, 10, RANDOM()) < 5, 'NW1 2DB', 'WC1E 6BT') AS POSTCODE_OK
  FROM base
),
codes AS (
  SELECT
    RN,
    IFF(UNIFORM(0, 10, RANDOM()) < 5, 'R1H12', 'R1K01') AS SITE_OK,
    IFF(UNIFORM(0, 10, RANDOM()) < 5, 'RJZ', 'RKE') AS TRUST_OK,
    IFF(UNIFORM(0, 10, RANDOM()) < 4, 'Ambulance', IFF(UNIFORM(0,10,RANDOM()) < 8, 'Walk-in', 'Other')) AS ARRIVAL_MODE_OK,
    IFF(UNIFORM(0,10,RANDOM()) < 3, 'Chest pain',
      IFF(UNIFORM(0,10,RANDOM()) < 6, 'Shortness of breath',
      IFF(UNIFORM(0,10,RANDOM()) < 8, 'Abdominal pain', 'Head injury'))) AS COMPLAINT_OK,
    IFF(UNIFORM(0,10,RANDOM()) < 3, 'R07.9',
      IFF(UNIFORM(0,10,RANDOM()) < 6, 'R06.0',
      IFF(UNIFORM(0,10,RANDOM()) < 8, 'R10.4', 'S09.90'))) AS DIAG_CODE_OK
  FROM base
),
diag AS (
  SELECT
    RN,
    CASE DIAG_CODE_OK
      WHEN 'R07.9' THEN 'Chest pain, unspecified'
      WHEN 'R06.0' THEN 'Dyspnoea'
      WHEN 'R10.4' THEN 'Other and unspecified abdominal pain'
      WHEN 'S09.90' THEN 'Unspecified injury of head, initial encounter'
      ELSE 'Unknown'
    END AS DIAG_DESC_OK
  FROM codes
),
assembled AS (
  SELECT
    t.RN,
    -- attendance id (inject duplicates every 50th row)
    IFF(MOD(t.RN, 50)=0, 'AE-' || LPAD((t.RN-1)::STRING, 6, '0'), 'AE-' || LPAD(t.RN::STRING, 6, '0')) AS ATTENDANCE_ID,

    -- Inject missing / short NHS number every 17th row
    IFF(MOD(t.RN, 17)=0, NULL,
      IFF(MOD(t.RN, 29)=0, '12345', p.NHS_NUMBER_OK)
    ) AS NHS_NUMBER,

    c.SITE_OK AS HOSPITAL_SITE_CODE,
    c.TRUST_OK AS TRUST_CODE,

    -- Inject impossible DOB (future) every 41st row
    IFF(MOD(t.RN, 41)=0, DATEADD('day', 10, CURRENT_DATE()), p.DOB_OK) AS PATIENT_DOB,

    -- Inject age out-of-range every 37th row
    IFF(MOD(t.RN, 37)=0, 210,
      IFF(MOD(t.RN, 19)=0, -3, p.AGE_OK)
    ) AS PATIENT_AGE,

    -- Inject invalid gender codes every 23rd row
    IFF(MOD(t.RN, 23)=0, 'X',
      IFF(MOD(t.RN, 31)=0, NULL, p.GENDER_OK)
    ) AS PATIENT_GENDER,

    -- Inject invalid postcode every 13th row
    IFF(MOD(t.RN, 13)=0, 'INVALID', p.POSTCODE_OK) AS PATIENT_POSTCODE,

    -- Inject future arrival timestamps every 53rd row
    IFF(MOD(t.RN, 53)=0, DATEADD('day', 7, t.ARRIVAL_TS), t.ARRIVAL_TS) AS ARRIVAL_TS,

    -- Inject triage before arrival every 47th row
    IFF(MOD(t.RN, 47)=0, DATEADD('minute', -15, t.ARRIVAL_TS), t.TRIAGE_TS) AS TRIAGE_TS,

    -- Inject clinician before triage every 43rd row
    IFF(MOD(t.RN, 43)=0, DATEADD('minute', -10, t.TRIAGE_TS), t.FIRST_CLINICIAN_TS) AS FIRST_CLINICIAN_TS,

    -- Inject missing discharge time every 11th row
    IFF(MOD(t.RN, 11)=0, NULL, t.DISCHARGE_TS) AS DISCHARGE_TS,

    -- Inject invalid arrival mode every 27th row
    IFF(MOD(t.RN, 27)=0, 'Teleport', c.ARRIVAL_MODE_OK) AS ARRIVAL_MODE,

    c.COMPLAINT_OK AS CHIEF_COMPLAINT,
    c.DIAG_CODE_OK AS DIAGNOSIS_CODE,
    d.DIAG_DESC_OK AS DIAGNOSIS_DESC,

    -- Wait mins (inject negative / huge values)
    IFF(MOD(t.RN, 21)=0, -5, DATEDIFF('minute', t.ARRIVAL_TS, t.TRIAGE_TS)) AS WAIT_MINS_TO_TRIAGE,
    IFF(MOD(t.RN, 33)=0, 99999, DATEDIFF('minute', t.ARRIVAL_TS, t.FIRST_CLINICIAN_TS)) AS WAIT_MINS_TO_CLINICIAN,

    -- LOS mins (if discharge null then null; inject negative every 39th row)
    IFF(MOD(t.RN, 39)=0, -60,
      IFF(t.DISCHARGE_TS IS NULL, NULL, DATEDIFF('minute', t.ARRIVAL_TS, t.DISCHARGE_TS))
    ) AS TOTAL_LOS_MINS,

    -- breach flag (inject invalid codes)
    IFF(MOD(t.RN, 25)=0, 'Maybe',
      IFF(DATEDIFF('minute', t.ARRIVAL_TS, t.DISCHARGE_TS) > 240, 'Y', 'N')
    ) AS FOUR_HOUR_BREACH_FLAG
  FROM ts t
  JOIN patients p ON p.RN = t.RN
  JOIN codes c ON c.RN = t.RN
  JOIN diag d ON d.RN = t.RN
)
SELECT
  ATTENDANCE_ID, NHS_NUMBER, HOSPITAL_SITE_CODE, TRUST_CODE,
  PATIENT_DOB, PATIENT_AGE, PATIENT_GENDER, PATIENT_POSTCODE,
  ARRIVAL_TS, TRIAGE_TS, FIRST_CLINICIAN_TS, DISCHARGE_TS,
  ARRIVAL_MODE, CHIEF_COMPLAINT, DIAGNOSIS_CODE, DIAGNOSIS_DESC,
  WAIT_MINS_TO_TRIAGE, WAIT_MINS_TO_CLINICIAN, TOTAL_LOS_MINS,
  FOUR_HOUR_BREACH_FLAG
FROM assembled;

-- Quick sanity checks
SELECT COUNT(*) AS RAW_ROWS FROM AE_ATTENDANCE_RAW;
SELECT COUNT(*) AS DISTINCT_ATTENDANCE_IDS FROM (SELECT DISTINCT ATTENDANCE_ID FROM AE_ATTENDANCE_RAW);
