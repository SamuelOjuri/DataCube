-- Synthetic data only. All IDs use E/C/H prefixes and are separate from copied data.
CREATE TABLE fixture_projects (
 monday_id text PRIMARY KEY,pipeline_stage text,category text,date_created date,
 new_enquiry_value numeric(12,2),total_order_value numeric(12,2),gestation_period integer);
INSERT INTO fixture_projects
SELECT v.id,v.stage,v.category,(date_trunc('month',c.as_of_date)+v.offset_days*interval '1 day')::date,
 v.enquiry,v.orders,v.gestation
FROM context c CROSS JOIN (VALUES
 ('E1','Won - Closed (Invoiced)','A',-30,100::numeric,100::numeric,10),
 ('E2','Lost','B',-1,200,200,0),
 ('E3','Open Enquiry','B',0,NULL,0,NULL),
 ('E4','Open Enquiry','B',-32,0,0,30),
 ('E5','Open Enquiry','B',-10,0,NULL,-1)
) v(id,stage,category,offset_days,enquiry,orders,gestation);
-- Fix month-boundary examples independently of month length.
UPDATE fixture_projects SET date_created=(SELECT (date_trunc('month',as_of_date)-interval '1 month')::date FROM context) WHERE monday_id='E1';
UPDATE fixture_projects SET date_created=(SELECT (date_trunc('month',as_of_date)-interval '1 month'-interval '1 day')::date FROM context) WHERE monday_id='E4';
CREATE TABLE fixture_hidden_items (
 monday_id text PRIMARY KEY,cust_order_value_material numeric(12,2),cust_additional_charges numeric(12,2));
INSERT INTO fixture_hidden_items VALUES ('H1',100,7.5),('H2',0,0),('H3',NULL,10);
CREATE TABLE fixture_subitems (
 monday_id text PRIMARY KEY,parent_monday_id text REFERENCES fixture_projects(monday_id),
 hidden_item_id text REFERENCES fixture_hidden_items(monday_id),reason_for_change text,
 quote_amount numeric(12,2),amount_invoiced numeric(12,2));
INSERT INTO fixture_subitems VALUES
 ('C1','E1','H1','New Enquiry',100,100),('C2','E1','H1','Revision',25,-20),
 ('C3','E2','H2','new enquiry',50,0),('C4','E3','H3',NULL,NULL,NULL);
