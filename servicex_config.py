import subprocess
import json
import logging
import psutil
import time

logger = logging.getLogger('servicex_logger')

def _is_chart_running(name: str):
    """
    Check whether a helm chart with `name` is running
    """
    result = subprocess.run(['helm', 'list', '--filter', name, '-q'], stdout=subprocess.PIPE)
    if result.returncode != 0:
        return False
    if result.stdout.decode('utf-8').strip() != name:
        return False
    return True


def _get_pod_status(name: str):
    """
    Get the pod status for everything that starts with name
    """
    result = subprocess.run(['kubectl', 'get', 'pod', '-o', 'json'], stdout=subprocess.PIPE)
    # print(result)
    data = json.loads(result.stdout)
    return [{'name': p['metadata']['name'], 'status': all([s['ready'] for s in p['status']['containerStatuses']])} for p in data['items'] if p['metadata']['name'].startswith(name)]


def _get_existing_transformers():
    result = subprocess.run(['kubectl', 'get', 'pod', '-o', 'json'], stdout=subprocess.PIPE)
    # print(result)
    data = json.loads(result.stdout)
    existing_transformers = len([p for p in data['items'] if p['metadata']['name'].startswith('transformer')])
    logger.debug(f'Number of existing transformer pods before make request: {existing_transformers}')


def _check_servicex_pods(name: str):
    """
    Checking helm chart of ServiceX and pod status
    """
    # if not _is_chart_running(name):
    #     raise BaseException(f"Helm chart is not deployed!")    
    status = _get_pod_status(name)
    is_ready = all(s['status'] for s in status)
    if not is_ready:
        raise BaseException(f"ServiceX is not ready! Pod(s) are not running.")
    logger.info(f'ServiceX is up and running!')
    

def _find_pod(helm_release_name:str, pod_name:str):
    """
    Find the pod name in the release and return the full name
    """
    pods = _get_pod_status(helm_release_name)
    named_pods = [p['name'] for p in pods if p['name'].startswith(f"{helm_release_name}-{pod_name}")]
    assert len(named_pods) == 1    
    return named_pods[0]


def _check_portforward_running(processName: str):
    """
    Iterate over the all the running process
    """
    for proc in psutil.process_iter():
        try:
            if "kubectl" in proc.name().lower() and processName in proc.cmdline()[2]:
                return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return False


def _connect_servicex_backend(name: str, app_name: str, port: int):
    """
    Connect ServiceX backend
    """
    if not _check_portforward_running(app_name):        
        try:
            subprocess.Popen(["kubectl", "port-forward", _find_pod(name, app_name), "{}:{}".format(port,port)], stdout=subprocess.DEVNULL)
            logger.info(f"Connect to the backend of {app_name}")
            time.sleep(2) # Wait until port-forward is being established             
        except:
            logger.info(f"Cannot connect to the backend of {app_name}")
    else:
        logger.info(f"Already connected to the backend of {app_name}")        


def _disconnect_servicex_backend():
    """
    Disconnect ServiceX backend
    """
    servicex_backend_pids = []
    for p in psutil.process_iter():
        try:
            all_process = p.name()
        except (psutil.AccessDenied, psutil.ZombieProcess):
            pass
        except psutil.NoSuchProcess:
            continue
        if all_process.lower() == "kubectl" and ("servicex-app" in p.cmdline()[2] or "minio" in p.cmdline()[2]):
            servicex_backend_pids.append(p.pid)

    for connection in servicex_backend_pids:
        try:
            logger.info( "Disconnected to the backend: " + psutil.Process(connection).cmdline()[2] )
            psutil.Process(connection).kill()
        except:
            logger.info( "Cannot disconnect to the backend: " + psutil.Process(connection).cmdline()[2] )