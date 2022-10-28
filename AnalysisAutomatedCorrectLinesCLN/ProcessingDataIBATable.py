# Скрипт экспорта таблиц воздействий из iba hd


from __future__ import print_function
import os.path
import datetime
import grpc
import ibaHD_API_pb2
import ibaHD_API_pb2_grpc
import pandas as pd

chanelname1 = ["ЦЛН\[9:250]", "ЦЛН\[9:251]", "ЦЛН\[9:252]", "ЦЛН\[9:253]", "ЦЛН\[9:254]", "ЦЛН\[9:255]",
               "ЦЛН\[9:256]", "ЦЛН\[9:257]", "ЦЛН\[9:258]", "ЦЛН\[9:259]", "ЦЛН\[9:260]", "ЦЛН\[9:261]",
               "ЦЛН\[9:262]", "ЦЛН\[9:263]", "ЦЛН\[9:264]", "ЦЛН\[9:265]", "ЦЛН\[9:266]", "ЦЛН\[9:267]",
               "ЦЛН\[9:268]", "ЦЛН\[9:269]"]
chanelname2 = ["ЦЛН\[10:250]", "ЦЛН\[10:251]", "ЦЛН\[10:252]", "ЦЛН\[10:253]", "ЦЛН\[10:254]", "ЦЛН\[10:255]",
               "ЦЛН\[10:256]", "ЦЛН\[10:257]", "ЦЛН\[10:258]", "ЦЛН\[10:259]", "ЦЛН\[10:260]", "ЦЛН\[10:261]",
               "ЦЛН\[10:262]", "ЦЛН\[10:263]", "ЦЛН\[10:264]", "ЦЛН\[10:265]", "ЦЛН\[10:266]", "ЦЛН\[10:267]",
               "ЦЛН\[10:268]", "ЦЛН\[10:269]"]
chanelname3 = ["ЦЛН\[11:15]", "ЦЛН\[11:16]", "ЦЛН\[11:17]", "ЦЛН\[11:18]", "ЦЛН\[11:19]", "ЦЛН\[11:20]",
               "ЦЛН\[11:21]", "ЦЛН\[11:22]", "ЦЛН\[11:23]", "ЦЛН\[11:24]", "ЦЛН\[11:25]", "ЦЛН\[11:26]",
               "ЦЛН\[11:27]", "ЦЛН\[11:28]", "ЦЛН\[11:29]", "ЦЛН\[11:30]", "ЦЛН\[11:31]", "ЦЛН\[11:32]",
               "ЦЛН\[11:33]", "ЦЛН\[11:34]"]

start_date = '11082022'
end_date = '11082022'
input_path = ''
install_repos = ''
line = 'LINE2'
input_path = os.path.join(input_path, line)

if line == 'LINE1':
    chanelname = chanelname1
elif line == 'LINE2':
    chanelname = chanelname2
elif line == 'LINE3':
    chanelname = chanelname3

local_path = f"{input_path}/DataPreprocessing_cln_CurvAnalysis_{start_date}_{end_date}"
folname = "curvtable"


class ApiKeyCallCredentials(grpc.AuthMetadataPlugin):

    def __init__(self, apikey):
        self._apikey = apikey

    def __call__(self, context, callback):
        metadata = (('ibahdapi-apikey', self._apikey),)

        callback(metadata, None)


certificate = open(install_repos + "/certificate_HD.crt", 'rb').read()
tls_credentials = grpc.ssl_channel_credentials(certificate)

apiKey = ''
apikey_credentials = grpc.metadata_call_credentials(ApiKeyCallCredentials(apiKey))
combined_credentials = grpc.composite_channel_credentials(tls_credentials, apikey_credentials)


def API_connect(channel, chanelname, start, end):
    client = ibaHD_API_pb2_grpc.HdApiServiceStub(channel)
    xparam = ibaHD_API_pb2.GetRawChannelDataRequest(channel_ids=chanelname,
                                                    time_range_from=start,
                                                    time_range_to=end,
                                                    max_sample_count_per_message=1200 if line == 'LINE3' else 120000
                                                    )
    return client.GetRawChannelData(request=xparam)


def write_file(data, path):
    count = 0
    val = [0 for i in chanelname if ':' in i]
    vald = [i[-6:] for i in chanelname if '.' in i]
    valtime = {}
    valdigit = {}
    for i in vald:
        valtime[i] = []
        valdigit[i] = []
    valtimefloat = []
    mytab = []
    old_val = []
    counttab = 1
    a1 = 'curvature'
    a2 = 'action'
    a = ['time']
    for i in range(1, 11):
        a.append(a1 + str(i))
        a.append(a2 + str(i))
    a.insert(0, 'dt_start')
    a.insert(1, 'dt_stop')
    mydf = pd.DataFrame(columns=a)
    for a in data:
        if a.float_values:
            data_val = round(a.float_values[1], 3)
            valuetimestamp = str(datetime.datetime.fromtimestamp(a.start_timestamp / 1e6))

            for i in range(len(val)):
                val[i] = data_val if count == i else val[i]
            if count == len(val) - 1:
                r = {}
                a1 = 'time'
                a2 = 'curvature'
                a3 = 'action'
                r[a1] = valuetimestamp
                count = 1
                add = 0

                for i, strr in enumerate(val):
                    if i % 2 == 0:
                        r[a2 + str(count)] = strr
                        add += 1
                    else:
                        r[a3 + str(count)] = strr
                        add += 1
                    if add == 2:
                        count += 1
                        add = 0
                if len(old_val) > 0:
                    if old_val != val:
                        counttab += 1
                r['tab'] = counttab

                mydf = mydf.append(r, ignore_index=True)
                mytab.append(valuetimestamp + ';' + ';'.join([str(i) for i in val]))
                valtimefloat.append(valuetimestamp[:19])
                count = 0
                old_val = val
                val = [0 for i in chanelname if ':' in i]
            else:
                count += 1
    a1 = 'curvature'
    a2 = 'action'
    a = []
    for i in range(1, 11):
        a.append(a1 + str(i))
        a.append(a2 + str(i))
    a.insert(0, 'dt_start')
    a.insert(1, 'dt_stop')

    g = mydf.groupby('tab').max()
    g['dt_start'] = mydf.groupby('tab').min()['time']
    g['dt_stop'] = mydf.groupby('tab').max()['time']
    g = g.drop(columns=['time'])
    g = g[a]
    g.to_csv(f"{path[:-12]}curvature_table.txt")


if not os.path.exists(os.path.join(local_path, folname)):
    os.mkdir(os.path.join(local_path, folname))
ipServer = ''
channel = grpc.secure_channel(ipServer, combined_credentials,
                              options=[('grpc.max_receive_message_length', 2147483647)])
path = f"{os.path.join(local_path, folname)}/{end_date}.txt"
start = int(
    datetime.datetime(int(start_date[4:]), int(start_date[2:4]), int(start_date[:2]), 00, 00, 40, 0).timestamp() * 1e6)
end = int(datetime.datetime(int(end_date[4:]), int(end_date[2:4]), int(end_date[:2]), 23, 59, 59, 0).timestamp() * 1e6)
write_file(API_connect(channel=channel, chanelname=chanelname, start=start, end=end), path)