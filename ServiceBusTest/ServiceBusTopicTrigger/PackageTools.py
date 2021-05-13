import json
import zlib

from numpyencoder import NumpyEncoder
from base64 import b64decode, b64encode
from datetime import datetime

def uncompress_samples(b64_str):
    try:
        compressed_str = b64decode(b64_str)
        encoded_str = zlib.decompress(compressed_str)
        packed_dict = json.loads(encoded_str)
    except Exception as e:
        print(e)
    else:
        return packed_dict

def packed_dict_to_list(packed_dict):
    list_samples = []
    list_keys = list(packed_dict.keys())
    if (type(packed_dict[list_keys[0]]) == str):            
        for key in list_keys:
            packed_dict[key] = json.loads(packed_dict[key])
    num_samples = len(list(packed_dict.values())[0])

    for i in range(0,num_samples):
        sample = {}
        for key in list_keys:
            sample[key] = packed_dict[key][i]
        list_samples += [sample]
        
    return list_samples

def decrypt_tstamp(tstamp):
    ts = int(tstamp)
    ts = ts / 1000
    ts = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    
    return ts