# Скрипт предобработки данных из iba hd

from __future__ import print_function
import datetime
import grpc
import ibaHD_API_pb2
import ibaHD_API_pb2_grpc
import os

chanelname1 = ["ЦЛН\[9:22]", "ЦЛН\[9:11]", "ЦЛН\[9:12]" , "ЦЛН\[9:8]", "ЦЛН\[9:10]", "ЦЛН\[9:23]", "ЦЛН\[9:46]", "ЦЛН\[9:164]", "ЦЛН\[9:170]", "ЦЛН\[9.61]", "ЦЛН\[9.62]", "ЦЛН\[9;163]", "ЦЛН\[9;162]"  ]
chanelname2 = ["ЦЛН\[10:22]", "ЦЛН\[10:11]", "ЦЛН\[10:12]" , "ЦЛН\[10:8]", "ЦЛН\[10:10]", "ЦЛН\[10:23]", "ЦЛН\[10:46]", "ЦЛН\[10:164]", "ЦЛН\[10:170]", "ЦЛН\[10.61]", "ЦЛН\[10.62]", "ЦЛН\[10;163]", "ЦЛН\[10;162]" ]
chanelname3 = ["ЦЛН\[11:187]", "ЦЛН\[11:122]", "ЦЛН\[11:124]" , "ЦЛН\[11:121]", "ЦЛН\[11:123]", "ЦЛН\[11:185]", "ЦЛН\[11:183]", "ЦЛН\[11:349]", "ЦЛН\[11.79]", "ЦЛН\[11.80]", "ЦЛН\[11;347]", "ЦЛН\[11;348]" ]

date = '11082022'                                                   # Дата для экспорта
local_path = ''                                                     # Путь к папке с проектом
install_repos = ''                                                  # Путь к HD папке
line = 'LINE2'                                                      # Номер обрабатываемой линии
local_path = os.path.join(local_path, line)

if line == 'LINE1':
	chanelname = chanelname1
elif line == 'LINE2':
	chanelname = chanelname2
elif line == 'LINE3':
	chanelname = chanelname3

if not os.path.exists(local_path):
	os.mkdir(local_path)

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
    xparam = ibaHD_API_pb2.GetRawChannelDataRequest(channel_ids = chanelname,
                                                        time_range_from = start,
                                                        time_range_to = end,
                                                        max_sample_count_per_message = 10
                                                        )
    return client.GetRawChannelData(request=xparam)

def digit_generator(valtimefloat, valtime, valdigit, result_digit):
    tempdigit = [None for _ in valtimefloat]
    for i in range(len(valtime)):
        for j in range(len(valtimefloat)):
            if valtimefloat[j] == valtime[i]:
                tempdigit[j] = valdigit[i]

    for i in range(len(tempdigit)-1, -1, -1):
        if not tempdigit[i] is None:
            if not tempdigit[i-1] is None or not tempdigit[i+1] is None:
                tempdigit[i] = None

    for i in range(len(tempdigit)):
        result_digit[i] += ',' + str(tempdigit[i])
    return result_digit

def write_file(data, path, val, vald):
    file_object = open(path, 'a')

    count = 0

    val = val
    vald = vald
    valtime = {}
    valdigit = {}
    for i in vald:
        valtime[i] = []
        valdigit[i] = []
    valtimefloat = []
    mytab = []
    for a in data:
        if (a.digital_values or a.string_values) and a.channel_id[-6:] in vald:
            tekid = a.channel_id[-6:]
            valtime[tekid] += [str(datetime.datetime.fromtimestamp(i / 1e6))[:19] for i in a.digital_values.timestamp] if '.' in tekid else [str(datetime.datetime.fromtimestamp(i / 1e6))[:19] for i in a.string_values.timestamp]
            valdigit[tekid] += [i for i in a.digital_values.value] if '.' in tekid else [i for i in a.string_values.value]

        if a.float_values or a.double_values:
            data_val = round(a.float_values[0], 3) if a.float_values else round(a.double_values[0], 3)
            valuetimestamp = str(datetime.datetime.fromtimestamp(a.start_timestamp / 1e6))

            for i in range(len(val)):
                val[i] = data_val if count == i else val[i]
            if count == len(val) - 1:
                mytab.append(valuetimestamp + ',' + ','.join([str(i) for i in val]))
                # file_object.write("\n")
                valtimefloat.append(valuetimestamp[:19])
                count = 0
                val = [0 for i in chanelname if ':' in i]
            else:
                count += 1


    res_tab = ['' for _ in valtimefloat]
    for i in vald:
        res_tab = digit_generator(valtimefloat, valtime[i], valdigit[i], res_tab)

    for i in range(len(mytab)):
        mytab[i] += str(res_tab[i])
        file_object.write(mytab[i])
        file_object.write('\n')

    file_object.close()

ipServer = ''
channel = grpc.secure_channel(ipServer, combined_credentials, options=[('grpc.max_receive_message_length', 2147483647)])

path = f"{local_path}/{date}.txt"
start = int(datetime.datetime(int(date[4:]), int(date[2:4]), int(date[:2]), 00, 00, 1, 0).timestamp() * 1e6)
end = int(datetime.datetime(int(date[4:]), int(date[2:4]), int(date[:2]), 23, 59, 59, 0).timestamp() * 1e6)
val = [0 for i in chanelname if ':' in i]
vald = [i[-6:].replace(';',':') for i in chanelname if '.' in i or ';' in i]

write_file(API_connect(channel=channel, chanelname=[i.replace(';',':') for i in chanelname], start=start, end=end), path, val, vald)
