/* prepare  encounter mapping
 * 1. add mapping for entity type
 * 2. add mapping for global type (aggregation type)
 */
SELECT e1.*, (CASE WHEN e1.submission_format ='Facility' THEN 2
 WHEN e1.submission_format ='Professional' THEN 1  ELSE 0 END ) entity_type, 
 (CASE WHEN e1.submission_format ='Facility' THEN 2
 WHEN e1.submission_format ='Professional' THEN 1  ELSE 0 END ) aggregation_type
INTO encounter_mapping_v4
FROM encounter_structure_v3 e1
--2723

UPDATE encounter_mapping a
SET aggregation_type=3
WHERE a.modifier=''
AND a.billing_code IN (
SELECT DISTINCT e.billing_code
FROM encounter_structure_v3 e
WHERE  e.modifier IN ('TC', '26'))


SELECT m.code_type, COUNT(DISTINCT M.billing_code)
FROM encounter_mapping m
GROUP BY m.code_type
'CPT', 484
'HCPCS', 17

/*  processing in network parquet data
 * 1. extract data by billing code
 * 2. data cleansing 
 */
--MRF USING file: 
s3://smrf-mrm/bcbskc/output/202303/2023-01-26_BKC_HPCRC0001_ffs_in-network.json.gz/in_network.parquet/
--total record: 12,429,138

-------data import: mrf_kc
SELECT count(*)
FROM mrf_kc
--7,255,907

--------------clean up data
-------------------check negotiation arrangement
SELECT m.negotiation_arrangement, m.negotiated_type, count(*)
FROM mrf_kc m
GROUP BY m.negotiation_arrangement, m.negotiated_type

'ffs', 'percentage', 194957
'ffs', 'fee schedule', 3260004
'ffs', 'negotiated', 3799835
'ffs', 'per diem', 1111

--------------check billing code type
SELECT m.billing_code_type, count(*), count(DISTINCT m.billing_code)
FROM mrf_kc m
GROUP BY m.billing_code_type
'CPT', 7077205, 464
'HCPCS', 178702, 16

/* data cleansing rules:
1 only include negotiation_arrangement ffs and negotiated_type Derived Fee schedule Negociated
2. exclude some billing code types, such as 'RC'
3. exclude all records with 0 <negociated rate<50,000
4. only include records with modifers in 'TC', '26', ''
*/
DROP TABLE mrf_kc_clean_v4;
SELECT *
INTO mrf_kc_clean_v4
FROM mrf_kc m
WHERE m.negotiation_arrangement='ffs'
AND m.negotiated_type IN ('fee schedule', 'negotiated', 'derived')
AND m.billing_code_type <>'RC' ---may add more in the future
AND m.negotiated_rates_max>0
and m.negotiated_rates_max<50000
--4,414,670

SELECT max(m.negotiated_rates_max), min(m.negotiated_rates_max)
FROM  mrf_kc_clean_v4 m
18,500, 0.01

------CREATE INDEX
CREATE INDEX idx_mrfkc4 ON mrf_kc_clean_v4(npi, billing_code);

--------------merge with npi data to get entity type and nucc
CREATE temp TABLE mrf_bcbskc_tmp AS 
SELECT t.*, n.entity_type_code, n.provider_organization_name, n.provider_other_organization_name,  n.last_name, n.first_name, 
n.provider_first_line_business_practice_location_address, n.provider_second_line_business_practice_location_address, n.provider_business_practice_location_address_city_name, 
n.provider_business_practice_location_address_state_name, n.provider_business_practice_location_address_postal_code
FROM mrf_kc_clean_v4 t, npi_2022 n
WHERE t.npi=n.npi_int

---link specialty in 
CREATE temp TABLE npi_specialty_tmp
as
SELECT x.npi, X.NPI_INT, ARRAY_agg(DISTINCT x.nucc) nucc_list, ARRAY_length(ARRAY_agg(DISTINCT x.nucc), 1) nucc_cnt, ARRAY_agg(DISTINCT t.classification) specialty_list,
ARRAY_agg(DISTINCT t.display_name) display_list
FROM (
SELECT m.npi, M.NPI_INT, nucc_list, REPLACE(json_array_elements(nucc_list)::VARCHAR, '"', '') AS nucc
FROM (
SELECT a.npi, a.npi_int, json_build_array(a.healthcare_provider_taxonomy_code_1, a.healthcare_provider_taxonomy_code_2, 
a.healthcare_provider_taxonomy_code_3, a.healthcare_provider_taxonomy_code_4, a.healthcare_provider_taxonomy_code_5,
a.healthcare_provider_taxonomy_code_6, a.healthcare_provider_taxonomy_code_7, a.healthcare_provider_taxonomy_code_8, a.healthcare_provider_taxonomy_code_9, a.healthcare_provider_taxonomy_code_10, a.healthcare_provider_taxonomy_code_11, 
a.healthcare_provider_taxonomy_code_12, a.healthcare_provider_taxonomy_code_13, a.healthcare_provider_taxonomy_code_14, 
a.healthcare_provider_taxonomy_code_15) nucc_list
FROM npi_2022 a, mrf_bcbskc_tmp n
WHERE n.npi=a.npi_int) m
) x, nucc_taxonomy t
WHERE x.nucc IS NOT NULL 
AND x.nucc<>'null'
AND x.nucc=t.code
GROUP BY x.npi, X.NPI_INT

---------------create merged data of mrf and npi specialty
DROP TABLE mrf_kc_merge_v4;
SELECT DISTINCT t.npi, t.npi_int, n.billing_code,  n.tin_value, n.billing_class, n.billing_code_modifier, n.service_codes, n.negotiation_arrangement, n.negotiated_type, n.tin_type, 
n.billing_code_type, n.negotiated_rates_list, n.negotiated_rates_max, n.negotiated_rates_min, n.name, n.billing_code_type_version, n.description, 
n.expiration_date, n.additional_information, n.bundled_codes, n.covered_services, n.entity_type_code, n.provider_organization_name, 
n.provider_other_organization_name, n.last_name, n.first_name, n.provider_first_line_business_practice_location_address, n.provider_second_line_business_practice_location_address, 
n.provider_business_practice_location_address_city_name, n.provider_business_practice_location_address_state_name, n.provider_business_practice_location_address_postal_code ,
t.nucc_List, t.nucc_cnt, t.specialty_List, t.display_list
INTO mrf_kc_merge_v4
FROM mrf_bcbskc_tmp n LEFT OUTER JOIN npi_specialty_tmp t
ON  n.npi=t.npi_int 
--4,372,290

---------------------filter by specialty
----## NOTE: check speciaty mapping table for empty space in front of specialty codes
DROP TABLE mrf_kc_match_v4;
SELECT DISTINCT m.*, s.specialty_code, s.name AS specialty_name
INTO mrf_kc_match_v4
FROM mrf_kc_merge_v4 m, specialty_billing_code_mapping s
WHERE ltrim(s.specialty_code)=ANY(m.nucc_list)
AND m.billing_code=ltrim(s.service_code)
---163343

---------business rule: rate from TC, 26 should be less than the global rate
DROP TABLE mrf_kc_match_filtered_v4;
SELECT k.*
INTO mrf_kc_match_filtered_v4
FROM mrf_kc_match_v4 k
WHERE NOT EXISTS (SELECT *
FROM (
SELECT b.npi, b.tin_value, b.billing_code, a.negotiated_rates_max, B.negotiated_rates_max, a.billing_code_modifier, b.billing_code_modifier,
b.service_codes, b.negotiation_arrangement, b.negotiated_type 
FROM 
(SELECT v.npi, v.tin_value, V.billing_code, v.billing_code_modifier, V.negotiated_rates_max, v.service_codes, v.negotiation_arrangement, v.negotiated_type 
FROM mrf_kc_match_v4 V
WHERE ''=ANY(V.billing_code_modifier)
AND V.billing_code IN (
SELECT DISTINCT M.billing_code
FROM mrf_kc_match_v4 m
WHERE 'TC'=ANY(m.billing_code_modifier) ))A,
(SELECT DISTINCT m.npi, m.tin_value, M.billing_code, m.billing_code_modifier, m.negotiated_rates_max, m.service_codes, m.negotiation_arrangement, m.negotiated_type 
FROM mrf_kc_match_v4 m
WHERE m.billing_code_modifier && ARRAY['TC', '26'] ) B
WHERE A.negotiated_rates_max<B.negotiated_rates_max
AND a.billing_code=b.billing_code
AND a.npi=b.npi
AND a.tin_value=b.tin_value
AND (a.service_codes && b.service_codes OR (a.service_codes IS NULL AND b.service_codes IS NULL ))
AND a.negotiation_arrangement=b.negotiation_arrangement
AND a.negotiated_type =b.negotiated_type) n 
WHERE n.npi = k.npi
AND n.tin_value=k.tin_value
AND n.billing_code=k.billing_code
AND k.billing_code_modifier && ARRAY['TC', '26'] 
AND n.service_codes && k.service_codes
AND n.negotiation_arrangement=k.negotiation_arrangement
AND n.negotiated_type =k.negotiated_type )
---163343

----------checking results
SELECT b.npi, b.tin_value, b.billing_code, a.negotiated_rates_max, B.negotiated_rates_max, a.billing_code_modifier, b.billing_code_modifier,
b.service_codes, b.negotiation_arrangement, b.negotiated_type 
FROM 
(SELECT v.npi, v.tin_value, V.billing_code, v.billing_code_modifier, V.negotiated_rates_max, v.service_codes, v.negotiation_arrangement, v.negotiated_type 
FROM mrf_kc_match_filtered_v3 V
WHERE ''=ANY(V.billing_code_modifier)
AND V.billing_code IN (
SELECT DISTINCT M.billing_code
FROM mrf_kc_match_v3 m
WHERE 'TC'=ANY(m.billing_code_modifier) ))A,
(SELECT DISTINCT m.npi, m.tin_value, M.billing_code, m.billing_code_modifier, m.negotiated_rates_max, m.service_codes, m.negotiation_arrangement, m.negotiated_type 
FROM mrf_kc_match_filtered_v3 m
WHERE m.billing_code_modifier && ARRAY['TC', '26'] ) B
WHERE A.negotiated_rates_max<B.negotiated_rates_max
AND a.billing_code=b.billing_code
AND a.npi=b.npi
AND a.tin_value=b.tin_value
AND a.service_codes && b.service_codes
AND a.negotiation_arrangement=b.negotiation_arrangement
AND a.negotiated_type =b.negotiated_type
--0

------check for missing codes and filter down the encounter code list
DROP TABLE mrf_kc_missing_check_v4;
SELECT DISTINCT m.*, k.billing_code mrf_bc, k.billing_code_type
INTO mrf_kc_missing_check_v4
FROM encounter_mapping_v4 m
LEFT OUTER JOIN mrf_kc_match_filtered_v4 k
ON m.billing_code=k.billing_code
---2714

---exclude at group level
DROP TABLE encounter_mapping_poc_v4; 
SELECT e.encounter_type, e.encounter_code, e.encounter_name, e.encounter_variation_code, e.encounter_variation_name, e.internal_service_code, e.encounter_group_code, e.code_type, e.billing_code, 
e.submission_format, e.modifier, e.priority, e.weight, e.entity_type, e.aggregation_type, e.min_cost, e.max_cost 
INTO encounter_mapping_poc_v4
FROM mrf_kc_missing_check_v4 e
WHERE NOT EXISTS (SELECT *
FROM mrf_kc_missing_check_v4 e1 
WHERE e1.mrf_bc IS NULL 
AND e1.encounter_group_code=e.encounter_group_code)
--1946

-----------merge encouter code table with mrf data for POC data by inner join
DROP TABLE mrf_kc_encounter_combine_v4;
SELECT DISTINCT P.encounter_type, P.encounter_code, p.encounter_name, p.encounter_variation_code, p.encounter_variation_name, p.encounter_group_code,
P.modifier, m.*, P.internal_service_code internal_code, P.priority, P.weight, p.aggregation_type
INTO mrf_kc_encounter_combine_v4  
FROM mrf_kc_match_filtered_v4 m, encounter_mapping_poc_v4 P
WHERE  m.billing_code=p.billing_code
AND m.entity_type_code::INT =p.entity_type
AND P.modifier=ANY(m.billing_code_modifier)
--881265

-----------------business rule update negatiated type for future grouping
UPDATE mrf_kc_encounter_combine_v4 
SET negotiated_type='DFN'
WHERE negotiated_type IN ('derived', 'fee schedule', 'negotiated')
AND negotiation_arrangement='ffs'

-------------------### 2 generate generic cost: bcbskc_mr_data_prep_generic4.sql
-------------getting generic cost by billing code: aggregate 
DROP TABLE mrf_kc_generic_bc_v4; 
SELECT m.encounter_type, m.encounter_code, m.encounter_name, M.encounter_variation_code, m.encounter_variation_name, m.encounter_group_code, 
m.billing_code_type,  m.billing_code, m.modifier,  m.entity_type_code, m.negotiation_arrangement, m.negotiated_type, max(m.negotiated_rates_max) max, min(m.negotiated_rates_max), 
avg(m.negotiated_rates_max) avg, PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY m.negotiated_rates_max)  med, 
count(*) record_cnt,   m.service_codes,  m.name, m.priority, m.weight, m.aggregation_type 
INTO mrf_kc_generic_bc_v4
FROM mrf_kc_encounter_combine_v4 m
GROUP BY m.encounter_type, m.encounter_code, m.encounter_name, M.encounter_variation_code, m.encounter_variation_name, m.encounter_group_code, 
m.billing_code_type,  m.billing_code, m.modifier,  m.entity_type_code, m.negotiation_arrangement, m.negotiated_type,   m.service_codes,  m.name, m.priority, m.weight, m.aggregation_type 
--3227

----------roll up to group code level while matching billingcode+modifier count with mapping
DROP TABLE mrf_kc_generic_grpcd_v4; 
SELECT n.encounter_type, n.encounter_code, n.encounter_name, n.encounter_variation_code, n.encounter_variation_name, n.encounter_group_code,
n.entity_type_code, n.priority, n.weight, n.aggregation_type , n.max, n.avg, n.med, n.min, n.negotiation_arrangement, n.negotiated_type, n.service_codes
INTO mrf_kc_generic_grpcd_v4
FROM 
(SELECT s.encounter_type, s.encounter_code, s.encounter_name, s.encounter_variation_code, s.encounter_variation_name, s.internal_service_code, s.encounter_group_code , 
s.submission_format, s.entity_type, s.aggregation_type,  count(concat(s.billing_code, s.modifier)) cnt
FROM encounter_mapping_poc_v4 s
GROUP BY s.encounter_type, s.encounter_code, s.encounter_name, s.encounter_variation_code, s.encounter_variation_name, s.internal_service_code, s.encounter_group_code , 
s.submission_format, s.entity_type, s.aggregation_type) m,
(SELECT g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.encounter_group_code,
g.entity_type_code, sum(g.max) max, sum(g.avg) avg, sum(g.med) med, sum(g.min) min, g.negotiation_arrangement, g.negotiated_type, g.service_codes, count(concat(g.billing_code, g.modifier)) cnt,
g.priority, g.weight, g.aggregation_type 
FROM mrf_kc_generic_bc_v4 g
GROUP BY g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.encounter_group_code,
g.entity_type_code, g.negotiation_arrangement, g.negotiated_type, g.service_codes, g.priority, g.weight, g.aggregation_type ) n
WHERE m.encounter_code= n.encounter_code
AND m.encounter_variation_code=n.encounter_variation_code
AND m.encounter_group_code=n.encounter_group_code
AND m.entity_type=n.entity_type_code::INT 
AND m.aggregation_type=n.aggregation_type
AND m.cnt=n.cnt
--2279

---------------roll up to variation level, including weight
DROP TABLE mrf_kc_generic_varcd_v4; 
SELECT g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.entity_type_code, g.aggregation_type, 
max(g.max) max, avg(g.avg) avg, sum(g.avg*g.weight)/sum(g.weight) w_avg,
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY g.med) med, min(g.min) min, g.negotiation_arrangement, g.negotiated_type, g.service_codes
INTO mrf_kc_generic_varcd_v4
FROM mrf_kc_generic_grpcd_v4 g
GROUP BY g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.entity_type_code, g.aggregation_type, 
g.negotiation_arrangement, g.negotiated_type, g.service_codes
--1046

SELECT *
FROM mrf_kc_generic_varcd_v4 v
WHERE v.avg<>v.w_avg

--------------------added: check agains max/min cost in encounter mapping
UPDATE  mrf_kc_generic_varcd_v4 v
SET max= (CASE WHEN v.max>e.max_cost THEN e.max_cost ELSE v.max END),
avg=(CASE WHEN v.avg>e.max_cost THEN e.max_cost ELSE v.avg END ),
w_avg=(CASE WHEN v.w_avg>e.max_cost THEN e.max_cost ELSE v.w_avg END ),
med=(CASE WHEN v.med>e.max_cost THEN e.max_cost ELSE v.med END ),
min=(CASE WHEN v.min>e.max_cost THEN e.max_cost ELSE v.min END )
FROM encounter_mapping_poc_v4 e
WHERE v.encounter_code=e.encounter_code
AND v.encounter_variation_code=e.encounter_variation_code

UPDATE  mrf_kc_generic_varcd_v4 v
SET max= (CASE WHEN v.max<e.min_cost THEN e.min_cost ELSE v.max END),
avg=(CASE WHEN v.avg<e.min_cost THEN e.min_cost ELSE v.avg END ),
w_avg=(CASE WHEN v.w_avg<e.min_cost THEN e.min_cost ELSE v.w_avg END ),
med=(CASE WHEN v.med<e.min_cost THEN e.min_cost ELSE v.med END ),
min=(CASE WHEN v.min<e.min_cost THEN e.min_cost ELSE v.min END )
FROM encounter_mapping_poc_v4 e
WHERE v.encounter_code=e.encounter_code
AND v.encounter_variation_code=e.encounter_variation_code

-------------process 2; filter out incomplete variation professional to facility combo, 
DROP TABLE mrf_kc_generic_varcd_2_v4; 
SELECT DISTINCT v1.*
INTO mrf_kc_generic_varcd_2_v4
FROM mrf_kc_generic_varcd_v4 v1
WHERE EXISTS (SELECT *
FROM (
SELECT g.encounter_variation_code, g.service_codes, count(DISTINCT g.entity_type_code) cnt1, count(DISTINCT v.entity_type) cnt2
FROM mrf_kc_generic_varcd_v4 g, encounter_mapping_poc_v4 v
WHERE g.aggregation_type<>3
AND v.encounter_variation_code=g.encounter_variation_code
GROUP BY g.encounter_variation_code, g.service_codes
HAVING count(DISTINCT g.entity_type_code)=count(DISTINCT v.entity_type) ) m
WHERE m.encounter_variation_code=v1.encounter_variation_code
AND m.service_codes && v1.service_codes)
AND v1.aggregation_type<>3
---780

--need to include global types
INSERT INTO mrf_kc_generic_varcd_2_v4
SELECT DISTINCT *
FROM mrf_kc_generic_varcd_v4 v1
WHERE v1.aggregation_type=3
--137

-----------ROLL UP contributing cost to variation level
DROP TABLE contributing_costs_var_v3;
SELECT DISTINCT t.encounter_type, t.encounter_code, t.encounter_name, t.encounter_variation_code, t.encounter_variation_name, 
sum(t.min) min,  sum(t.max) max, sum(t.average) average, sum(t.median) median
INTO contributing_costs_var_v3
FROM contributing_cost_v3 t
GROUP BY t.encounter_type, t.encounter_code, t.encounter_name, t.encounter_variation_code, t.encounter_variation_name

--------------final combined generic cost with 3 entity types 0, 1, 2
DROP TABLE mrf_kc_generic_v4; 
SELECT m.*
INTO mrf_kc_generic_v4
FROM (SELECT *
FROM mrf_kc_generic_varcd_2_v4
UNION 
SELECT DISTINCT t.encounter_type, t.encounter_code, t.encounter_name, t.encounter_variation_code, t.encounter_variation_name, '0' entity_type, 0 aggregation_type, 
t.max, t.average, t.average, t.median,t.min, v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM contributing_costs_var_v3 t, mrf_kc_generic_varcd_2_v4 v
WHERE t.encounter_variation_code=v.encounter_variation_code
AND t.encounter_variation_code=v.encounter_variation_code) m
--970

SELECT *
FROM mrf_kc_generic_v4 g
ORDER  BY g.service_codes, g.encounter_variation_code, g.entity_type_code, g.aggregation_type

-------------------### 3. Generate Individual cost: bcbskc_mr_data_prep_individual4.sql
---start with mrf_kc_encounter_combine npi, tin and billing code level
SELECT a.entity_type_code, count(DISTINCT a.npi)
FROM mrf_kc_encounter_combine_v4 a
GROUP BY a.entity_type_code
'1', 2715
'2', 57

--------------aggregate individual rate at billing code level 
 DROP TABLE mrf_kc_indv_bc_v4; 
 SELECT  g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.encounter_group_code  , g.npi, g.npi_int, g.tin_value, 
g.service_codes, g.negotiation_arrangement, g.negotiated_type,
max(g.negotiated_rates_max) max, min(g.negotiated_rates_max), avg(g.negotiated_rates_max) avg, 
PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY g.negotiated_rates_max)  med, g.entity_type_code, g.weight, g.aggregation_type,  g.priority, 
 g.provider_organization_name, g.provider_other_organization_name, g.last_name, g.first_name, 
 g.provider_first_line_business_practice_location_address, g.provider_second_line_business_practice_location_address, 
 g.provider_business_practice_location_address_city_name, g.provider_business_practice_location_address_state_name, 
 g.provider_business_practice_location_address_postal_code, g.internal_code, g.billing_code, g.modifier, g.billing_code_type 
 INTO mrf_kc_indv_bc_v4
 FROM mrf_kc_encounter_combine_v4 g 
GROUP BY g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.encounter_group_code  , g.npi, g.npi_int, g.tin_value, 
g.service_codes, g.negotiation_arrangement, g.negotiated_type, g.tin_type, g.entity_type_code, 
 g.provider_organization_name, g.provider_other_organization_name, g.last_name, g.first_name, 
 g.provider_first_line_business_practice_location_address, g.provider_second_line_business_practice_location_address, 
 g.provider_business_practice_location_address_city_name, g.provider_business_practice_location_address_state_name, 
 g.provider_business_practice_location_address_postal_code, g.internal_code, g.priority, g.weight, g.aggregation_type, 
 g.billing_code, g.modifier, g.billing_code_type 
 --849,006

----------aggregate billing code rate to group code level while match mapping count
DROP TABLE mrf_kc_indv_grpcd_v4;
SELECT DISTINCT n.encounter_type, n.encounter_code, n.encounter_name, n.encounter_variation_code, n.encounter_variation_name, n.encounter_group_code , n.npi, n.npi_int, n.tin_value, 
n.service_codes, n.negotiation_arrangement, n.negotiated_type, n.max, n.avg, n.med, n.min, n.entity_type_code, n.aggregation_type,  n.weight, 
 n.provider_organization_name, n.provider_other_organization_name, n.last_name, n.first_name, 
 n.provider_first_line_business_practice_location_address, n.provider_second_line_business_practice_location_address, 
 n.provider_business_practice_location_address_city_name, n.provider_business_practice_location_address_state_name, 
 n.provider_business_practice_location_address_postal_code, n.internal_code
INTO mrf_kc_indv_grpcd_v4
FROM 
(SELECT s.encounter_type, s.encounter_code, s.encounter_name, s.encounter_variation_code, s.encounter_variation_name , s.encounter_group_code, s.submission_format, 
s.entity_type , s.aggregation_type,  count(concat(s.billing_code, s.modifier)) cnt
FROM encounter_mapping_poc_v4 s
GROUP BY s.encounter_type, s.encounter_code, s.encounter_name, s.encounter_variation_code, s.encounter_variation_name , s.encounter_group_code, s.submission_format, 
s.entity_type , s.aggregation_type) m,
(SELECT  g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.encounter_group_code  , g.npi, g.npi_int, g.tin_value, 
g.service_codes, g.negotiation_arrangement, g.negotiated_type,
sum(g.max) max, sum(g.min) min, sum(g.avg) avg, sum(g.med) med, g.entity_type_code, g.weight, g.aggregation_type,  
 g.provider_organization_name, g.provider_other_organization_name, g.last_name, g.first_name, 
 g.provider_first_line_business_practice_location_address, g.provider_second_line_business_practice_location_address, 
 g.provider_business_practice_location_address_city_name, g.provider_business_practice_location_address_state_name, 
 g.provider_business_practice_location_address_postal_code, g.internal_code, count(concat(g.billing_code, g.modifier)) cnt
 FROM mrf_kc_indv_bc_v4 g  
GROUP BY g.encounter_type, g.encounter_code, g.encounter_name, g.encounter_variation_code, g.encounter_variation_name, g.encounter_group_code  , g.npi, g.npi_int, g.tin_value, 
g.service_codes, g.negotiation_arrangement, g.negotiated_type,  g.entity_type_code, 
 g.provider_organization_name, g.provider_other_organization_name, g.last_name, g.first_name, 
 g.provider_first_line_business_practice_location_address, g.provider_second_line_business_practice_location_address, 
 g.provider_business_practice_location_address_city_name, g.provider_business_practice_location_address_state_name, 
 g.provider_business_practice_location_address_postal_code, g.internal_code, g.weight, g.aggregation_type) n
WHERE m.encounter_code= n.encounter_code
AND m.encounter_variation_code=n.encounter_variation_code
AND m.encounter_group_code=n.encounter_group_code
AND m.entity_type=n.entity_type_code::INT 
AND m.aggregation_type=n.aggregation_type
AND m.cnt=n.cnt
---336293

 ----------aggregate to variation code level
 DROP TABLE mrf_kc_indv_varcd_v4;
 SELECT n.encounter_type, n.encounter_code, n.encounter_name, n.encounter_variation_code, n.encounter_variation_name , n.npi, n.npi_int, n.tin_value, 
n.service_codes, n.negotiation_arrangement, n.negotiated_type, n.entity_type_code, n.aggregation_type,
max(n.max) max, avg(n.max) avg,  sum(n.max*n.weight)/sum(n.weight) w_avg,  PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY n.max) med , min(n.max) min, 
 n.provider_organization_name, n.provider_other_organization_name, n.last_name, n.first_name, 
 n.provider_first_line_business_practice_location_address, n.provider_second_line_business_practice_location_address, 
 n.provider_business_practice_location_address_city_name, n.provider_business_practice_location_address_state_name, 
 n.provider_business_practice_location_address_postal_code, n.internal_code
 INTO mrf_kc_indv_varcd_v4
 FROM mrf_kc_indv_grpcd_v4 n
 GROUP BY n.encounter_type, n.encounter_code, n.encounter_name, n.encounter_variation_code, n.encounter_variation_name , n.npi, n.npi_int, n.tin_value, 
n.service_codes, n.negotiation_arrangement, n.negotiated_type, n.entity_type_code, n.aggregation_type,
 n.provider_organization_name, n.provider_other_organization_name, n.last_name, n.first_name, 
 n.provider_first_line_business_practice_location_address, n.provider_second_line_business_practice_location_address, 
 n.provider_business_practice_location_address_city_name, n.provider_business_practice_location_address_state_name, 
 n.provider_business_practice_location_address_postal_code, n.internal_code
 --94734
 
 SELECT a.entity_type_code, count(DISTINCT a.npi)
FROM mrf_kc_indv_varcd_v4 a
GROUP BY a.entity_type_code
'1', 2715
'2', 57


