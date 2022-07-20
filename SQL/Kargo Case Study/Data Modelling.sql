-- Data Modelling --

with combine1 as(
    select activity_id, shipment_id, shipment_status, partnership.*
    from `kargotech-data.case_study.base_shipments` as base
    inner join `kargotech-data.case_study.partnership_pricings` as partnership
    on base.partnership_pricing_id = partnership.partnership_pricing_id
)

, company as(
select activity_id, comp.company_id, company_name
from combine1
inner join `kargotech-data.case_study.companies` as comp
using (company_id)
)

, partners as(
    select activity_id, comp.company_id as partners_id,
    company_name as partners_name
    from combine1
    inner join `kargotech-data.case_study.companies` as comp
    on combine1.partners_id = comp.company_id
)

, combine2 as(
    select company_name, partners_name, combine1.*
    from combine1
    inner join company
    using(activity_id)
    inner join partners
    using(activity_id)
)

, origin as(
    select location_id as origins_district_id,
    location_country as origin_country,
    location_province as origin_province,
    location_district as origin_district,
    location_sub_district as origin_sub_district
    from `kargotech-data.case_study.locations` as loc
)

, dest as(
    select location_id as dest_district_id,
    location_country as dest_country,
    location_province as dest_province,
    location_district as dest_district,
    location_sub_district as dest_sub_district
    from `kargotech-data.case_study.locations` as loc
)

, combine3 as(
    select origins_district_id,
    origin_country,
    origin_province,
    origin_district,
    origin_sub_district,
    dest_district_id,
    dest_country,
    dest_province,
    dest_district,
    dest_sub_district,
    combine2.*
    from combine2
    inner join origin
    on combine2.origin_district_id = origin.origins_district_id
    inner join dest
    on combine2.destination_district_id = dest_district_id
)

, activity_arrive_at_origin as(
    select activity_id, inserted_at as arrive_at_origin_at
    from `kargotech-data.case_study.activity_logs`
    where timestamp_type = "arrive_at_origin_at"
)

, activity_finish_loading as(
    select activity_id, inserted_at as finish_loading_at
    from `kargotech-data.case_study.activity_logs`
    where timestamp_type = "finish_loading_at"
)

, activity_on_shipment as(
    select activity_id, inserted_at as on_shipment_at
    from `kargotech-data.case_study.activity_logs`
    where timestamp_type = "on_shipment_at"
)

, activity_arrive_at_destination as(
    select activity_id, inserted_at as arrive_at_destination_at
    from `kargotech-data.case_study.activity_logs`
    where timestamp_type = "arrive_at_destination_at"
)

, activity_finish_unloading as(
    select activity_id, inserted_at as finish_unloading_at
    from `kargotech-data.case_study.activity_logs`
    where timestamp_type = "finish_unloading_at"
)

, activity_completed as(
    select activity_id, inserted_at as completed_at
    from `kargotech-data.case_study.activity_logs`
    where timestamp_type = "completed_at"
)

, activity as(
    select finish_loading_at,
    on_shipment_at,
    arrive_at_destination_at,
    finish_unloading_at,
    completed_at,
    activity_arrive_at_origin.*
    from activity_arrive_at_origin
    full join activity_finish_loading
    using(activity_id)
    full join activity_on_shipment
    using(activity_id)
    full join activity_arrive_at_destination
    using(activity_id)
    full join activity_finish_unloading
    using(activity_id)
    full join activity_completed
    using(activity_id)
)

, combine4 as(
    select arrive_at_origin_at,
    finish_loading_at,
    on_shipment_at,
    arrive_at_destination_at,
    finish_unloading_at,
    completed_at,
    combine3.*
    from combine3
    inner join activity
    on combine3.activity_id = activity.activity_id
)

SELECT shipment_id,
shipment_status,
partnership_pricing_id,
company_id,
company_name,
partners_id,
partners_name,
origin_district_id,
origin_country,
origin_province,
origin_district,
origin_sub_district,
dest_district_id,
dest_country,
dest_province,
dest_district,
dest_sub_district,
arrive_at_origin_at,
finish_loading_at,
on_shipment_at,
arrive_at_destination_at,
finish_unloading_at,
completed_at,
price
FROM combine4