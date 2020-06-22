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


def _load_requests(inputDIDs: str):
    """
    Prepare requests for ServiceX
    """
    with open(inputDIDs) as f:        
        did_list = ['.'.join(line.split(".")[:2])+':'+line.rstrip() for line in f] # Add scope to DID
        

    request_list = []    
    for did in did_list:

        selection = "n_jets >= 0"
        variable = "n_jets"
        request = {}
        request["did"] = did
        request["tree-name"] = "NOMINAL"
        request["selection"] = tcut_to_qastle( selection, variable )
        request["image"] = "kyungeonchoi/servicex_pyroot_transformer:0.1"
        request["result-destination"] = "object-store"    
        request["result-format"] = "parquet"
        request["chunk-size"] = "1000"
        request["workers"] = "2"

        request_list.append(request)   

    return request_list


def print_requests(request: str, request_id):
    console_print = {"Input DID": request["did"], \
                    "Request ID": request_id}
    print(json.dumps(console_print, indent=4))
    request_log = request.copy()
    # del request_log['selection']
    logger.debug(json.dumps(request_log, indent=4))


# def make_requests(request_list, full_config):
def _make_requests(request_list):
    """
    Make transform request
    """    
    _get_existing_transformers()
    request_id_list = []
    did_list = []
    for req in request_list:        
        response = requests.post("http://localhost:5000/servicex/transformation", json=req)  
        request_id = response.json()["request_id"]
        request_id_list.append( request_id )
        did_list.append(req['did'])
        print_requests(req, request_id) # print simplified request query

    return request_id_list, did_list

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