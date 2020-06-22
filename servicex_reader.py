# ServiceX to deliver histogram out of flat ntuple at grid for ttH ML analysis
# Requires Kubernetes cluster and ServiceX has to be deployed

import logging
# import requests
from servicex_config import _check_servicex_pods, _connect_servicex_backend, _disconnect_servicex_backend
from servicex_request import _load_requests, _make_requests, _monitor_multiple_requests
from servicex_postprocessing import _download_output_files
from servicex_timer_logger import time_measure, logger

# from ServiceX
inputDIDs = 'test_list.txt'
base_outpath = '/Users/kchoi/Work/UTAustin/Computing/ServiceX/ServiceXforNtupleMaker/V03_new/mc/hadhad/mc16e/nom/'


# Load logger
logger = logging.getLogger('servicex_logger')


# Initialize timer
t = time_measure("servicex")
t.set_time("start")


# Helm chart name of ServiceX
servicex_helm_chart_name = "uproot"


# Step 1: Check SerivceX pods
_check_servicex_pods(servicex_helm_chart_name)
# t.set_time("t_check_servicex_pods")


# Step 2: Prepare backend to make a request
_connect_servicex_backend(servicex_helm_chart_name, "servicex-app", 5000)
# t.set_time("t_connect_servicex_app")


# Step 3: Prepare transform requests
servicex_request_list = _load_requests(inputDIDs)
# t.set_time("t_prepare_request")


# Step 4: Make requests
request_id_list, did_list = _make_requests(servicex_request_list)
# t.set_time("t_make_request")


# Step 5: Monitor jobs
_monitor_multiple_requests(request_id_list)
# t.set_time("t_request_complete")


# Step 6: Prepare backend to download output
_connect_servicex_backend(servicex_helm_chart_name, "minio", 9000)
# t.set_time("t_connect_minio")


# Step 7: Download output
_download_output_files(request_id_list, did_list, base_outpath)
# t.set_time("t_download_outputs")


# Step 8: Post processing
# output_to_histogram(servicex_request_list[0], full_config, request_id_list, sample_list)
# t.set_time("t_postprocessing")


# Step 9: Disconnect from backends
_disconnect_servicex_backend()
# t.set_time("t_disconnect_apps")

# t.print_times()