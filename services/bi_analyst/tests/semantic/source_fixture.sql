-- Synthetic boundary cases only; source field types match repository SQL.
CREATE TABLE projects (monday_id text PRIMARY KEY,pipeline_stage text,category text,type text,
 account text,product_type text,date_created date,date_order_received date,first_date_designed date,
 first_date_invoiced date,new_enquiry_value numeric(12,2),total_order_value numeric(12,2),
 total_amount_invoiced numeric(12,2),gestation_period integer);
CREATE VIEW reportable_projects WITH(security_invoker=true) AS SELECT * FROM projects WHERE monday_id<>'excluded';
CREATE TABLE hidden_items (monday_id text PRIMARY KEY,cust_order_value_material numeric(12,2),
 cust_additional_charges numeric(12,2),amount_invoiced numeric(12,2));
CREATE TABLE subitems (monday_id text PRIMARY KEY,parent_monday_id text,hidden_item_id text,
 reason_for_change text,quote_amount numeric(12,2),new_enquiry_value numeric(12,2),
 amount_invoiced numeric(12,2),invoice_date date);
CREATE TABLE analysis_results (id uuid PRIMARY KEY,project_id text,analysis_timestamp timestamptz,
 expected_conversion_rate numeric(3,2),expected_gestation_days integer);
CREATE VIEW context AS SELECT current_date AS as_of_date;
INSERT INTO projects(monday_id,pipeline_stage,category,type,account,product_type,date_created,
 date_order_received,new_enquiry_value,total_order_value,total_amount_invoiced,gestation_period)
SELECT id,stage,category,'New Build','A, B','P, Q',
 (date_trunc('month',current_date)+days*interval '1 day')::date,
 (date_trunc('month',current_date)-interval '1 month')::date,enquiry,orders,invoices,gestation
FROM (VALUES
 ('E1','Won - Closed (Invoiced)',' A ',-1,100::numeric,100::numeric,80::numeric,10),
 ('E2','Lost','B',-1,200,200,0,0),
 ('E3','Open Enquiry','B',0,NULL,0,NULL,NULL),
 ('E4',NULL,' ',-1,0,NULL,-5,-1),
 ('future','Won - Open (Order Received)','A',100,50,50,20,30),
 ('excluded','Won - Closed (Invoiced)','A',-1,999,999,999,999)
) v(id,stage,category,days,enquiry,orders,invoices,gestation);
INSERT INTO hidden_items VALUES ('H1',100,7.5,100),('H2',0,0,-20),('H3',NULL,10,NULL);
INSERT INTO subitems VALUES
 ('C1','E1','H1','New Enquiry',100,100,100,(date_trunc('month',current_date)-interval '1 month')::date),
 ('C2','E1','H1','Revision',25,0,-20,(date_trunc('month',current_date)-interval '1 month')::date),
 ('C3','E2','H2','new enquiry',50,0,0,current_date),
 ('C4','E3','H3',NULL,NULL,NULL,NULL,NULL),
 ('C5','E4',NULL,'New Enquiry',NULL,NULL,-5,NULL),
 ('C6','future','missing','New Enquiry',50,50,20,current_date),
 ('C7','excluded','H2','New Enquiry',999,999,999,current_date);
INSERT INTO analysis_results VALUES
 ('00000000-0000-0000-0000-000000000001','E1','2026-01-01T00:00:00Z',.2,10),
 ('00000000-0000-0000-0000-000000000002','E1','2026-01-01T00:00:00Z',.8,10),
 ('00000000-0000-0000-0000-000000000003','E1',NULL,.9,10);
