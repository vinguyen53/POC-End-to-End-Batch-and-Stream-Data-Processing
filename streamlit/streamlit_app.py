import streamlit as st
import requests
import json
import time
import pandas as pd
import matplotlib.pyplot as plt
from streamlit_autorefresh import st_autorefresh

#var
username = 'admin'
password = 'Tavi@102743'
headers = {'content-type':'application/json'}
dremioServer = 'http://dremio:9047'

#Title
st.title('People Dashboard')

#---------------Using Dremio REST API for query data-----------------#

#function api get token authen
def login(username, password):
  # we login using the old api for now
  loginData = {'userName': username, 'password': password}
  response = requests.post(f'{dremioServer}/apiv2/login', headers=headers, data=json.dumps(loginData))
  data = json.loads(response.text)

  # retrieve the login token
  token = data['token']
  return {'content-type':'application/json', 'authorization':'_dremio{authToken}'.format(authToken=token)}

#get token authen
headers = login(username, password)
#print('token: ',headers)

#function api post query
def apiPost(endpoint, body=None):
  text = requests.post('{server}/api/v3/{endpoint}'.format(server=dremioServer, endpoint=endpoint), headers=headers, data=json.dumps(body)).text

  # a post may return no data
  if (text):
    return json.loads(text)
  else:
    return None

#function api get result after post query
def apiGet(endpoint):
  return json.loads(requests.get('{server}/api/v3/{endpoint}'.format(server=dremioServer, endpoint=endpoint), headers=headers).text)

#funtion get job id after post query
def querySQL(query):
  queryResponse = apiPost('sql', body={'sql': query})
  jobid = queryResponse['id']
  return jobid

#function get data
def get_data(query_sql, query_name):
    job_id = querySQL(query_sql)
    #print('job id: ', jobid)

    time.sleep(0.2)

    max_time = 30
    sleep_duration = 0.1
    job_state = 'RUNNING'
    while (max_time > 0 and job_state not in ['COMPLETED', 'CANCELED', 'FAILED']):
        max_time = max_time - sleep_duration
        status = json.loads(requests.get(f'{dremioServer}/api/v3/job/{job_id}', headers=headers).text)
        job_state = status.get('jobState')
        time.sleep(sleep_duration)
    print(f'{query_name} status: ', status)

    if job_state == 'COMPLETED':
        results = apiGet('job/{id}/results?offset={offset}&limit={limit}'.format(id=job_id, offset=0, limit=100))

        #print(results.get('rows'))
        return results.get('rows')
    else:
       return None

#query 1
query_1 = "SELECT sex, count(id) as number_count FROM nessie.iceberg_stream.people GROUP BY sex"
df = pd.DataFrame(get_data(query_sql=query_1, query_name='query_1'))
print(df)
#---bar chart---#
st.bar_chart(df.set_index('sex')['number_count'], color = '#669933')
#---pie chart---#
pie_chart = df.set_index('sex').plot.pie(y='number_count', autopct='%1.1f%%', figsize=(7,  7))
st.pyplot(pie_chart.get_figure())

st_autorefresh(interval=30000)