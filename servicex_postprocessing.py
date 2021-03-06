import uproot
# import awkward
import pyarrow.parquet as pq
import numpy
import coffea
import os
import re
import logging
from minio import Minio
# from ServiceXRequests import read_configuration, load_requests
# from ServiceXConfig import connect_servicex_backend, disconnect_servicex_backend
# from ServiceXRequests import read_configuration

logger = logging.getLogger('servicex_logger')

def _download_output_files(request_id_list, did_list, base_outpath):
    if len([x for x in request_id_list if x is not None]):
        minio_endpoint = "localhost:9000"
        minio_client = Minio(minio_endpoint,
            access_key='miniouser',
            secret_key='leftfoot1',
            secure=False)
        for request_id, did in zip(request_id_list, did_list):
            if request_id is not None:
                objects = minio_client.list_objects(request_id)
                sample_files = list([file.object_name for file in objects])    
                for i in range(len(sample_files)):
                    minio_client.fget_object(request_id, sample_files[i], f'{base_outpath}/{did.split(":")[1]}/{sample_files[i].split(":")[-1]}')
    else:
        return None


# def get_lumi(full_config):
#     lumi = full_config['Job0']['Lumi'].split('%')[0].strip()
#     if re.findall(r'(XXX_\w+)',lumi):
#         replacements = re.findall(r'(XXX_\w+)',lumi)
#         replacement_file = full_config['Job0']['ReplacementFile']
#         with open( replacement_file ) as replacementFile:
#             for line in enumerate(replacementFile.readlines()):
#                 for xxx in replacements:
#                     if re.search(rf'{xxx}\b', line[1]):
#                         lumi = re.sub(xxx, line[1].strip(xxx + ":").strip(), lumi)
#     return lumi


# def output_to_histogram(servicex_request, full_config, request_id_list, sample_list):
#     if len([x for x in request_id_list if x is not None]):
#         if servicex_request["result-format"] == "parquet":
#             # logger.info(f'Merge files from request id: {request_id}')

#             # output_file per REGION
#             output_file_name = full_config["Job0"]["Job"] + "/Histograms/" + full_config["Job0"]["InputName"]+"_"+full_config['Region0']['Region'] + "_histos.root"
#             fout = uproot.recreate( output_file_name )

#             binFromVariable = False
#             try:
#                 full_config['Region0']['Binning'].split(",")
#                 binFromVariable = True
#                 logger.info(f'Histogram binning from "Region/Variable"')
#             except KeyError:
#                 logger.info(f'Histogram binning from "Region/Binning"')

#             if binFromVariable:
#                 hist_binning = full_config['Region0']['Binning'].split(",")            
#             else:
#                 start = float(full_config['Region0']['Variable'].split(",")[2])
#                 stop = float(full_config['Region0']['Variable'].split(",")[3])
#                 step = (stop-start)/int(full_config['Region0']['Variable'].split(",")[1])
#                 hist_binning = [x for x in numpy.arange(start, stop, step)]

#             # histograms for SAMPLE in REGION
#             for (request_id, sample) in zip(request_id_list, sample_list):
#                 hist_name = full_config["Region0"]["Region"] + "_" + sample
#                 h = coffea.hist.Hist(hist_name, coffea.hist.Bin("var", "", hist_binning))
#                 for file in os.listdir(full_config["Job0"]["Job"]+'/Histograms'):                     
#                     if request_id in file:           
#                         columns = pq.read_table(full_config["Job0"]["Job"]+"/Histograms/"+file)
#                         h.fill(var=numpy.array(columns.column(0)))
#                         # h.scale(float(get_lumi(full_config)))
#                         os.remove(full_config["Job0"]["Job"]+"/Histograms/"+file)
#                 h.scale(float(get_lumi(full_config))) # Normalize to the luminosity
#                 fout[hist_name] = coffea.hist.export1d(h)
                   
#             fout.close()
#             logger.info(f'Output file is created at: {output_file_name}')
#     else:
#         return None