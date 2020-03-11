from simple_salesforce import SFType
from SF_client import Salesforce
from S3_client import SimpleStorage
from datetime import date, datetime
import pandas as pd


sf=Salesforce()

table_list = ['Opportunity', 'Account', 'User', 'Contract', 'Case']

s3 = SimpleStorage('rt-salesforce-data')




def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)

for table in table_list:


    '''Pull fields from Saleforce Object'''
    fields = SFType(table,
                    sf.session_id,
                    sf.sf_instance,
                    sf.sf_version,
                    sf.proxies).describe()

    field_names = [field['name']
                   for field
                   in fields['fields']]

    '''Pull the date of last query to get look_back value'''
    try:
        new_table = False
        last_updated_bucket = s3.test_get_obj(table).get('Contents')[0].get('Key')

        last_query_time = last_updated_bucket.split('/')[1][0:10]
        look_back = days_between(str(date.today()), last_query_time)

        soql =  "SELECT {fields} " \
                "FROM {table} " \
                "WHERE LastModifiedDate=LAST_N_DAYS:{look_back} ".format(
                                                                    fields=','.join(field_names),
                                                                    table=table,
                                                                    look_back=look_back
        )

    except:
        '''Pull all historical data if it is a new table'''
        new_table = True
        soql = "SELECT {fields} " \
                "FROM {table} " .format(
                                        fields=','.join(field_names),
                                        table=table,
        )

    results = sf.execute_query(soql)
    sf_df = pd.DataFrame(results['records']).drop(columns='attributes')

    s3_target_path = '{table}/{date}.csv'.format(
                                            table = table,
                                            date = date.today())

    if new_table == False:
        s3.delete_object(last_updated_bucket)

    s3.create_object(s3_target_path,sf_df)












