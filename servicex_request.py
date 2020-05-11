import requests
import re
import linecache
import json
import time
import logging
import asyncio
from tqdm import tqdm
from servicex_tcut_to_qastle_wrapper import tcut_to_qastle
from servicex_config import _get_existing_transformers
from servicex_timer_logger import _write_transformer_log

logger = logging.getLogger('servicex_logger')


# def load_requests(confFile: str):
def _load_requests():    
    """
    Prepare requests for ServiceX
    """

    request_list = []    
    # for sam in samples:
    # selection = get_selection(confFile, sam)
    # variable = full_config['Region0']['Variable'].split(",")[0]
    selection = "tau_0_leadTrk_pt > 0"
    variable = "tau_0_leadTrk_pt, tau_1_leadTrk_pt, vbf_central_jet_type, vbf_central_jet_cleanJet_EC_LooseBad"
    request = {}
    request["did"] = "group.phys-higgs:group.phys-higgs.Htt_hh_V03.mc16_13TeV.410470.PhPy8_A14_ttb_nonallh.D3.e6337_s3126_r9364_p3978.smPre_w_0_HS"
    request["tree-name"] = "NOMINAL"
    # request["selection"],request["branches-in-selection"] = tcut_to_qastle( selection, variable )
    request["selection"] = tcut_to_qastle( selection, variable )
    # request["image"] = "sslhep/servicex_func_adl_uproot_transformer:pandas_to_arrow"
    request["image"] = "sslhep/servicex_func_adl_uproot_transformer:develop"
    request["result-destination"] = "object-store"    
    request["result-format"] = "parquet"
    request["chunk-size"] = "1000"
    request["workers"] = "6"
    # request["tcut-selection"] = selection
    request_list.append(request)   

    # return (request_list, full_config)
    return request_list


def get_request_info(request: str, full_config):
    """
    Return sample name associated to the request id
    """
    for x, y in full_config.items():
        if 'Sample' in x:
            for key in y:
                if y[key] == request["did"]:
                    return y['Sample'].strip()


def print_requests(request: str, full_config):
    console_print = {"Input DID": request["did"], \
                    "Tree": request["tree-name"], \
                    "Region": full_config["Region0"]["Region"], \
                    "Variable": full_config['Region0']['Variable'].split(",")[0]}
    print(json.dumps(console_print, indent=4))
    request_log = request.copy()
    del request_log['selection']
    logger.debug(json.dumps(request_log, indent=4))


# def make_requests(request_list, full_config):
def _make_requests(request_list):
    """
    Make transform request
    """    
    _get_existing_transformers()
    request_id_list = []
    sample_list = []
    for req in request_list:
        # print_requests(req, full_config) # print simplified request query
        # sample = get_request_info(req, full_config)

        # del req['branches-in-selection']
        # del req['tcut-selection']
        # print(json.dumps(req, indent=4))
        response = requests.post("http://localhost:5000/servicex/transformation", json=req)  
        request_id = response.json()["request_id"]
        request_id_list.append( request_id )
        # sample_list.append(sample)
        # else:
        #     print("Do NOT submit above ServiceX transform requests")
        #     request_id_list.append( None )
        #     sample_list.append( None )
        #     # return None
    # for req, sam in zip(request_id_list, sample_list):
    #     logger.info(f"Sample: {sam} - request id: {req}")
    # return request_id_list, sample_list
    return request_id_list

# async def monitor_requests(request_id, sample, pos:int):
async def _monitor_requests(request_id, pos:int):
    """
    Monitor jobs
    """
    if request_id == None:
        return None
    else:
        status_endpoint = f"http://localhost:5000/servicex/transformation/{request_id}/status"
        running = False
        while not running:
            status = requests.get(status_endpoint).json()
            files_remaining = status['files-remaining']
            files_processed = status['files-processed']
            if files_processed is not None and files_remaining is not None:
                running = True
            else:
                print('Status: Creating transformer pods...', end='\r')
        status = requests.get(status_endpoint).json() 
        files_remaining = status['files-remaining']
        files_processed = status['files-processed']
        total_files = files_remaining + files_processed
        t = tqdm(total=total_files, unit='file', desc=request_id, position=pos, leave=False)
        job_done = False
        while not job_done:       
            t.refresh()             
            await asyncio.sleep(1)
            status = requests.get(status_endpoint).json() 
            t.update(status['files-processed'] - t.n)
            files_remaining = status['files-remaining']
            files_processed = total_files-files_remaining
            if files_remaining is not None:
                if files_remaining == 0:
                    job_done = True
                    t.close()
                    _write_transformer_log(request_id)
        return request_id + " - " +  str(status['files-processed']) + "/" + str(total_files) + " files are processed"


# def _monitor_multiple_requests(request_id_list, sample_list):
def _monitor_multiple_requests(request_id_list):
    loop = asyncio.get_event_loop()
    # request_list = [monitor_requests(req, sam, i) for (req, sam, i) in zip(request_id_list, sample_list, range(len(request_id_list)))]
    request_list = [_monitor_requests(req, i) for (req, i) in zip(request_id_list, range(len(request_id_list)))]
    jobs = asyncio.wait(request_list)
    output,_ = loop.run_until_complete(jobs)
    print("\n")
    for job in output:
        # print(f"Finished jobs: {job.result()}")
        logger.info(f"Finished jobs: {job.result()}")
    loop.close()