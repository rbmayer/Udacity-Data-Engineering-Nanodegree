# DROP TABLES

drug_events_table_drop = "DROP TABLE IF EXISTS drug_events"
reactions_table_drop = "DROP TABLE IF EXISTS reactions"
safetyreports_table_drop = "DROP TABLE IF EXISTS safety_reports"
labels_table_drop = "DROP TABLE IF EXISTS labels"
pricing_table_drop = "DROP TABLE IF EXISTS pricing"
ndc_table_drop = "DROP TABLE IF EXISTS ndc"


# CREATE TABLES

drug_events_table_create = ("""CREATE TABLE IF NOT EXISTS drug_events (
    drug_event_id int IDENTITY primary key, 
    formatted_ndc nvarchar(11) NOT NULL sortkey,
    package_ndc nvarchar(20) NOT NULL,
    safetyreportid nvarchar(9) NOT NULL distkey
    )
    DISTSTYLE KEY;
""")

reactions_table_create = ("""CREATE TABLE IF NOT EXISTS reactions (
    reactionmeddrapt nvarchar(max) NOT NULL,
    reactions_id int IDENTITY primary key, 
    safetyreportid varchar(9) NOT NULL sortkey
    )
    DISTSTYLE AUTO;
""")

safetyreports_table_create = ("""CREATE TABLE IF NOT EXISTS safety_reports (
    id int IDENTITY,
    patientdeath BOOL,
    receiptdate date, 
    receivedate date, 
    safetyreportid varchar(9) NOT NULL primary key sortkey distkey, 
    seriousnesscongenitalanomali BOOL,
    seriousnessdeath BOOL,
    seriousnessdisabling BOOL,
    seriousnesshospitalization BOOL,
    seriousnesslifethreatening BOOL
    )
    DISTSTYLE KEY;
""")


labels_table_create = ("""CREATE TABLE IF NOT EXISTS labels (
    active_ingredient nvarchar(max),
    adverse_reactions nvarchar(max),
    brand_name nvarchar(max),
    drug_interactions nvarchar(max),
    formatted_ndc nvarchar(11) NOT NULL sortkey,
    generic_name nvarchar(max),
    indications_and_usage nvarchar(max),
    labels_id int IDENTITY,
    package_ndc nvarchar(20),
    warnings nvarchar(max)
    )
    DISTSTYLE AUTO;
""")


pricing_table_create = ("""CREATE TABLE IF NOT EXISTS pricing (
    as_of_date timestamp distkey,
    classification_for_rate_setting nvarchar,
    corresponding_generic_drug_effective_date timestamp,
    corresponding_generic_drug_nadac_per_unit float,
    effective_date timestamp,
    explanation_code nvarchar,
    nadac_per_unit float, 
    ndc nvarchar(11) NOT NULL sortkey,
    ndc_description nvarchar(max),
    otc nvarchar,
    pharmacy_type_indicator nvarchar, 
    pricing_id int IDENTITY primary key,
    pricing_unit nvarchar
    )
    DISTSTYLE KEY;
""")

ndc_table_create = ("""CREATE TABLE IF NOT EXISTS ndc (
    ndc varchar(11) NOT NULL primary key sortkey
    )
    DISTSTYLE ALL;
""")


# ALTER TABLES TO ADD FOREIGN CONSTRAINTS
drug_events_table_alter = ("""
    ALTER TABLE drug_events ADD foreign key(formatted_ndc) references ndc(ndc);
    ALTER TABLE drug_events ADD foreign key(safetyreportid) references safety_reports(safetyreportid);
    """)

reactions_table_alter = ("""
    ALTER TABLE reactions ADD foreign key(safetyreportid) references safety_reports(safetyreportid);
    """)

labels_table_alter = ("""
    ALTER TABLE labels ADD foreign key(formatted_ndc) references ndc(ndc);
    """)

pricing_table_alter = ("""
    ALTER TABLE pricing ADD foreign key(ndc) references ndc(ndc);
    """)

# POPULATE ndc TABLE AFTER DATABASE LOAD
create_labels_ndc_table = ("""CREATE TABLE IF NOT EXISTS public.labels_ndc (ndc varchar(11) NOT NULL sortkey);""")

insert_labels_ndc_query = ("""INSERT INTO public.labels_ndc (select distinct formatted_ndc as ndc from public.labels);""")

create_pricing_ndc_table = ("""CREATE TABLE IF NOT EXISTS public.pricing_ndc (ndc varchar(11) NOT NULL sortkey);""")

insert_pricing_ndc_query = ("""INSERT INTO public.pricing_ndc (select distinct ndc from public.pricing);""")

insert_ndc_query = ("""INSERT INTO public.ndc (select distinct ndc from (select * from public.labels_ndc union select * from public.pricing_ndc));""")

drop_labels_ndc = ("""DROP TABLE public.labels_ndc;""")

drop_pricing_ndc = ("""DROP TABLE public.pricing_ndc;""")

# QUERY LISTS
create_table_queries = [drug_events_table_create, reactions_table_create,
                        safetyreports_table_create, labels_table_create,
                        pricing_table_create, ndc_table_create]

drop_table_queries = [drug_events_table_drop, reactions_table_drop,
                      safetyreports_table_drop, labels_table_drop,
                      pricing_table_drop, ndc_table_drop]

alter_table_queries = [drug_events_table_alter, reactions_table_alter,
                       labels_table_alter, pricing_table_alter]

load_ndc_queries = [create_labels_ndc_table, insert_labels_ndc_query,
                    create_pricing_ndc_table, insert_pricing_ndc_query,
                    insert_ndc_query, drop_labels_ndc, drop_pricing_ndc]

