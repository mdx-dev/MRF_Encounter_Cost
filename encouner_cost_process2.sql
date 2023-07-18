/**
Combining data below:
Take individual professional record and link to generic facility with contributing cost per encounter mapping specification
Take individual facility record and link to generic professional with contributing cost per encounter mapping specification
Professional individual only with contributing cost per encounter mapping
Facility individual only with added contributing cost per encounter mapping specification
Global type with entity type as professional (aggregation type=3 and entity type =1) with added contributing cost per encounter mapping specification
Global type with entity type as facility (aggregation type=3 and entity type =2) with added contributing cost per encounter mapping specification
**/

--step 1
SELECT p.encounter_type, p.encounter_code, p.encounter_name, p.encounter_variation_code, p.encounter_variation_name, p.internal_code,
p.npi prof_npi, p.tin_value prof_tin, p.max prof_max, p.avg prof_avg, p.w_avg prof_w_avg, p.med prof_med, p.min prof_min, 
p.last_name, p.first_name, p.provider_first_line_business_practice_location_address prof_address1, 
p.provider_second_line_business_practice_location_address prof_address2, p.provider_business_practice_location_address_city_name prof_city, 
p.provider_business_practice_location_address_state_name prof_state, p.provider_business_practice_location_address_postal_code prof_zip, 
NULL facility_npi, NULL facility_tin, f.max facility_max, f.avg facility_avg, f.w_avg faclity_w_avg, f.med facility_med, f.min facility_min, 
'Generic Facility' facility_name, NULL facility_other_name, NULL facility_address1, 
NULL facility_address2, NULL  facility_city, NULL facility_state, NULL facility_zip, c.max contributing_max, c.average contributing_avg, 
c.median contributing_med, c.min contributing_min, p.negotiation_arrangement, p.negotiated_type, p.service_codes
FROM 
(SELECT  v.encounter_type, v.encounter_code, v.encounter_name, v.encounter_variation_code, v.encounter_variation_name, v.npi, 
v.tin_value, v.entity_type_code, v.max , v.avg , v.w_avg , v.med , v.min , v.last_name, v.first_name, 
v.provider_organization_name, v.provider_other_organization_name, v.provider_first_line_business_practice_location_address, 
v.provider_second_line_business_practice_location_address, v.provider_business_practice_location_address_city_name, 
v.provider_business_practice_location_address_state_name, v.provider_business_practice_location_address_postal_code, v.internal_code,
v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM mrf_kc_indv_varcd_v3 v
WHERE v.entity_type_code='1'
AND v.aggregation_type<>3
and v.encounter_variation_code IN (SELECT DISTINCT p.encounter_variation_code
FROM encounter_mapping_poc_v3 p
GROUP BY p.encounter_variation_code HAVING count(DISTINCT p.entity_type)=2) ) p
INNER JOIN 
(SELECT f.encounter_code, f.encounter_variation_code, f.max, f.avg, f.w_avg, f.med, f.min, f.negotiation_arrangement, f.negotiated_type, f.service_codes 
FROM mrf_kc_generic_varcd_v3 f
WHERE f.entity_type_code='2'
AND f.aggregation_type<>3
AND f.encounter_variation_code IN (SELECT DISTINCT p.encounter_variation_code
FROM encounter_mapping_poc_v3 p
GROUP BY p.encounter_variation_code HAVING count(DISTINCT p.entity_type)=2) ) f
ON p.encounter_code=f.encounter_code
AND p.encounter_variation_code=f.encounter_variation_code
AND p.negotiation_arrangement=f.negotiation_arrangement
AND p.negotiated_type=f.negotiated_type
AND p.service_codes && f.service_codes
LEFT OUTER JOIN contributing_costs_var_v3 c
ON p.encounter_code=c.encounter_code
AND p.encounter_variation_code=c.encounter_variation_code
UNION
----step2 
SELECT f.encounter_type, f.encounter_code, f.encounter_name, f.encounter_variation_code, f.encounter_variation_name, f.internal_code,
NULL  prof_npi, NULL prof_tin, p.max prof_max, p.avg prof_avg, p.w_avg prof_w_avg, p.med prof_med, p.min prof_min, 
'Generic' last_name, NULL first_name, NULL prof_address1, 
NULL prof_address2, NULL prof_city, NULL prof_state, NULL prof_zip, 
f.npi facility_npi, f.tin_value facility_tin, f.max facility_max, f.avg facility_avg, f.w_avg faclity_w_avg, f.med facility_med, f.min facility_min, 
f.provider_organization_name facility_name, f.provider_other_organization_name facility_other_name, f.provider_first_line_business_practice_location_address facility_address1, 
f.provider_second_line_business_practice_location_address facility_address2, f.provider_business_practice_location_address_city_name facility_city, 
f.provider_business_practice_location_address_state_name facility_state, f.provider_business_practice_location_address_postal_code facility_zip, 
c.max contributing_max, c.average contributing_avg, c.median contributing_med, c.min contributing_min, p.negotiation_arrangement, p.negotiated_type, p.service_codes
FROM 
(SELECT  v.encounter_type, v.encounter_code, v.encounter_name, v.encounter_variation_code, v.encounter_variation_name, v.npi, 
v.tin_value, v.entity_type_code, v.max , v.avg , v.w_avg , v.med , v.min , v.last_name, v.first_name, 
v.provider_organization_name, v.provider_other_organization_name, v.provider_first_line_business_practice_location_address, 
v.provider_second_line_business_practice_location_address, v.provider_business_practice_location_address_city_name, 
v.provider_business_practice_location_address_state_name, v.provider_business_practice_location_address_postal_code, v.internal_code,
v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM mrf_kc_indv_varcd_v3 v
WHERE v.entity_type_code='2'
AND v.aggregation_type<>3
and v.encounter_variation_code IN (SELECT DISTINCT p.encounter_variation_code
FROM encounter_mapping_poc_v3 p
GROUP BY p.encounter_variation_code HAVING count(DISTINCT p.entity_type)=2) ) f
INNER JOIN 
(SELECT f.encounter_code, f.encounter_variation_code, f.max, f.avg, f.w_avg, f.med, f.min, f.negotiation_arrangement, f.negotiated_type, f.service_codes 
FROM mrf_kc_generic_varcd_v3 f
WHERE f.entity_type_code='1'
AND f.aggregation_type<>3
AND f.encounter_variation_code IN (SELECT DISTINCT p.encounter_variation_code
FROM encounter_mapping_poc_v3 p
GROUP BY p.encounter_variation_code HAVING count(DISTINCT p.entity_type)=2) ) p
ON p.encounter_code=f.encounter_code
AND p.encounter_variation_code=f.encounter_variation_code
AND p.negotiation_arrangement=f.negotiation_arrangement
AND p.negotiated_type=f.negotiated_type
AND p.service_codes && f.service_codes
LEFT OUTER JOIN contributing_costs_var_v3 c
ON p.encounter_code=c.encounter_code
AND p.encounter_variation_code=c.encounter_variation_code
UNION
--step 3
SELECT p.encounter_type, p.encounter_code, p.encounter_name, p.encounter_variation_code, p.encounter_variation_name, p.internal_code,
p.npi prof_npi, p.tin_value prof_tin, p.max prof_max, p.avg prof_avg, p.w_avg prof_w_avg, p.med prof_med, p.min prof_min, 
p.last_name, p.first_name, p.provider_first_line_business_practice_location_address prof_address1, 
p.provider_second_line_business_practice_location_address prof_address2, p.provider_business_practice_location_address_city_name prof_city, 
p.provider_business_practice_location_address_state_name prof_state, p.provider_business_practice_location_address_postal_code prof_zip, 
NULL facility_npi, NULL facility_tin, 0 facility_max, 0 facility_avg, 0 faclity_w_avg, 0 facility_med, 0 facility_min, 
NULL facility_name, NULL facility_other_name, NULL facility_address1, 
NULL facility_address2, NULL  facility_city, NULL facility_state, NULL facility_zip, c.max contributing_max, c.average contributing_avg, 
c.median contributing_med, c.min contributing_min, p.negotiation_arrangement, p.negotiated_type, p.service_codes
FROM 
(SELECT  v.encounter_type, v.encounter_code, v.encounter_name, v.encounter_variation_code, v.encounter_variation_name, v.npi, 
v.tin_value, v.entity_type_code, v.max , v.avg , v.w_avg , v.med , v.min , v.last_name, v.first_name, 
v.provider_organization_name, v.provider_other_organization_name, v.provider_first_line_business_practice_location_address, 
v.provider_second_line_business_practice_location_address, v.provider_business_practice_location_address_city_name, 
v.provider_business_practice_location_address_state_name, v.provider_business_practice_location_address_postal_code, v.internal_code,
v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM mrf_kc_indv_varcd_v3 v
WHERE v.entity_type_code='1'
AND v.aggregation_type<>3
and v.encounter_variation_code IN (SELECT DISTINCT p.encounter_variation_code
FROM encounter_mapping_poc_v3 p
GROUP BY p.encounter_variation_code HAVING count(DISTINCT p.entity_type)=1) ) p
LEFT OUTER JOIN contributing_costs_var_v3 c
ON p.encounter_code=c.encounter_code
AND p.encounter_variation_code=c.encounter_variation_code
UNION
---step 4
SELECT f.encounter_type, f.encounter_code, f.encounter_name, f.encounter_variation_code, f.encounter_variation_name, f.internal_code,
NULL  prof_npi, NULL prof_tin, 0  prof_max, 0 prof_avg, 0 prof_w_avg, 0 prof_med, 0 prof_min, 
NULL last_name, NULL first_name, NULL prof_address1, 
NULL prof_address2, NULL prof_city, NULL prof_state, NULL prof_zip, 
f.npi facility_npi, f.tin_value facility_tin, f.max facility_max, f.avg facility_avg, f.w_avg faclity_w_avg, f.med facility_med, f.min facility_min, 
f.provider_organization_name facility_name, f.provider_other_organization_name facility_other_name, f.provider_first_line_business_practice_location_address facility_address1, 
f.provider_second_line_business_practice_location_address facility_address2, f.provider_business_practice_location_address_city_name facility_city, 
f.provider_business_practice_location_address_state_name facility_state, f.provider_business_practice_location_address_postal_code facility_zip, 
c.max contributing_max, c.average contributing_avg, c.median contributing_med, c.min contributing_min, f.negotiation_arrangement, f.negotiated_type, f.service_codes
FROM 
(SELECT  v.encounter_type, v.encounter_code, v.encounter_name, v.encounter_variation_code, v.encounter_variation_name, v.npi, 
v.tin_value, v.entity_type_code, v.max , v.avg , v.w_avg , v.med , v.min , v.last_name, v.first_name, 
v.provider_organization_name, v.provider_other_organization_name, v.provider_first_line_business_practice_location_address, 
v.provider_second_line_business_practice_location_address, v.provider_business_practice_location_address_city_name, 
v.provider_business_practice_location_address_state_name, v.provider_business_practice_location_address_postal_code, v.internal_code,
v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM mrf_kc_indv_varcd_v3 v
WHERE v.entity_type_code='2'
AND v.aggregation_type<>3
and v.encounter_variation_code IN (SELECT DISTINCT p.encounter_variation_code
FROM encounter_mapping_poc_v3 p
GROUP BY p.encounter_variation_code HAVING count(DISTINCT p.entity_type)=1) ) f
LEFT OUTER JOIN contributing_costs_var_v3 c
ON f.encounter_code=c.encounter_code
AND f.encounter_variation_code=c.encounter_variation_code
UNION 
----step 5
SELECT p.encounter_type, p.encounter_code, p.encounter_name, p.encounter_variation_code, p.encounter_variation_name, p.internal_code,
p.npi prof_npi, p.tin_value prof_tin, p.max prof_max, p.avg prof_avg, p.w_avg prof_w_avg, p.med prof_med, p.min prof_min, 
p.last_name, p.first_name, p.provider_first_line_business_practice_location_address prof_address1, 
p.provider_second_line_business_practice_location_address prof_address2, p.provider_business_practice_location_address_city_name prof_city, 
p.provider_business_practice_location_address_state_name prof_state, p.provider_business_practice_location_address_postal_code prof_zip, 
NULL facility_npi, NULL facility_tin, 0 facility_max, 0 facility_avg, 0 faclity_w_avg, 0 facility_med, 0 facility_min, 
NULL facility_name, NULL facility_other_name, NULL facility_address1, 
NULL facility_address2, NULL  facility_city, NULL facility_state, NULL facility_zip, c.max contributing_max, c.average contributing_avg, 
c.median contributing_med, c.min contributing_min, p.negotiation_arrangement, p.negotiated_type, p.service_codes
FROM 
(SELECT  v.encounter_type, v.encounter_code, v.encounter_name, v.encounter_variation_code, v.encounter_variation_name, v.npi, 
v.tin_value, v.entity_type_code, v.max , v.avg , v.w_avg , v.med , v.min , v.last_name, v.first_name, 
v.provider_organization_name, v.provider_other_organization_name, v.provider_first_line_business_practice_location_address, 
v.provider_second_line_business_practice_location_address, v.provider_business_practice_location_address_city_name, 
v.provider_business_practice_location_address_state_name, v.provider_business_practice_location_address_postal_code, v.internal_code,
v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM mrf_kc_indv_varcd_v3 v
WHERE v.entity_type_code='1'
AND v.aggregation_type=3) p
LEFT OUTER JOIN contributing_costs_var_v3 c
ON p.encounter_code=c.encounter_code
AND p.encounter_variation_code=c.encounter_variation_code
UNION
--------step 6
SELECT f.encounter_type, f.encounter_code, f.encounter_name, f.encounter_variation_code, f.encounter_variation_name, f.internal_code,
NULL  prof_npi, NULL prof_tin, 0  prof_max, 0 prof_avg, 0 prof_w_avg, 0 prof_med, 0 prof_min, 
NULL last_name, NULL first_name, NULL prof_address1, 
NULL prof_address2, NULL prof_city, NULL prof_state, NULL prof_zip, 
f.npi facility_npi, f.tin_value facility_tin, f.max facility_max, f.avg facility_avg, f.w_avg faclity_w_avg, f.med facility_med, f.min facility_min, 
f.provider_organization_name facility_name, f.provider_other_organization_name facility_other_name, f.provider_first_line_business_practice_location_address facility_address1, 
f.provider_second_line_business_practice_location_address facility_address2, f.provider_business_practice_location_address_city_name facility_city, 
f.provider_business_practice_location_address_state_name facility_state, f.provider_business_practice_location_address_postal_code facility_zip, 
c.max contributing_max, c.average contributing_avg, c.median contributing_med, c.min contributing_min, f.negotiation_arrangement, f.negotiated_type, f.service_codes
FROM 
(SELECT  v.encounter_type, v.encounter_code, v.encounter_name, v.encounter_variation_code, v.encounter_variation_name, v.npi, 
v.tin_value, v.entity_type_code, v.max , v.avg , v.w_avg , v.med , v.min , v.last_name, v.first_name, 
v.provider_organization_name, v.provider_other_organization_name, v.provider_first_line_business_practice_location_address, 
v.provider_second_line_business_practice_location_address, v.provider_business_practice_location_address_city_name, 
v.provider_business_practice_location_address_state_name, v.provider_business_practice_location_address_postal_code, v.internal_code,
v.negotiation_arrangement, v.negotiated_type, v.service_codes
FROM mrf_kc_indv_varcd_v3 v
WHERE v.entity_type_code='2'
AND v.aggregation_type=3) f
LEFT OUTER JOIN contributing_costs_var_v3 c
ON f.encounter_code=c.encounter_code
AND f.encounter_variation_code=c.encounter_variation_code