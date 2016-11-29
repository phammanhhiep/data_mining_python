# from collect_data.collect_orders import collect_orders
# from collect_data.collect_estore_spending import collect_estore_spending
# from collect_data.collect_estore_bg import collect_estore_bg
# from collect_data.collect_estore_contracts_erp import collect_estore_contracts_erp
# from collect_data.collect_other_erp import collect_company_bg, collect_dept_bg , collect_customer_bg
# from collect_data.collect_estore_traffic import collect_estore_traffic
from collect_data.collect_staff_bg_erp import collect_staff_bg_erp
# from collect_data.collect_staff_activities_erp import collect_staff_activities_erp

# start_date,end_date = ['2015-1-1', '2016-10-31']
# start_date,end_date = ['2016-10-1', '2016-10-31'] # testing

# eloginnames = [] # testing
# eids = [34,4391980] # testing

# eloginnames = ['aaaa','bbbb','cc'] # testing
# eid = [] # testing

# STOP HERE. collect unique estore ids, and collect bg according to the bg

# collect_orders(start_date, end_date, output_name='test_order')
# collect_estore_spending(start_date, end_date, get_eids=True)
# collect_estore_bg(get_eids=True, get_eids_args=[start_date,end_date], max_query=500)
# collect_estore_contracts_erp(from_date='2016-11-1')
# collect_company_bg()
# collect_dept_bg(319)
# collect_customer_bg()
# collect_estore_traffic()
collect_staff_bg_erp()
# collect_staff_activities_erp()