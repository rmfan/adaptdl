import argparse
import copy
import kubernetes
import os
import random

from time import sleep
from yaml import load

kubernetes.config.load_incluster_config()
custom_api = kubernetes.client.CustomObjectsApi()
core_api = kubernetes.client.CoreV1Api()
NAMESPACE = "default"

parser = argparse.ArgumentParser(description='AdaptDLJob chaos controller')
parser.add_argument('--jobs-per-cycle', type=int, default=10)
parser.add_argument('--cycle-interval', type=int, default=10)
parser.add_argument('--duration', type=int, default=60)
parser.add_argument('--image-error-rate', type=float, default=0.1)
parser.add_argument('--name-error-rate', type=float, default=0.1)
parser.add_argument('--job-path', type=str, required=True)
args = parser.parse_args()


def create_adaptdljob(yaml_path, error_type):
    with open(yaml_path, "r") as yaml_f:
        yaml = load(yaml_f)
    if "image" in error_type:
        del(yaml["spec"]["template"]["spec"]["containers"][0]["image"])
    if "name" in error_type:
        del(yaml["spec"]["template"]["spec"]["containers"][0]["name"])
    return custom_api.create_namespaced_custom_object(
        "adaptdl.petuum.com", "v1", NAMESPACE, "adaptdljobs", yaml)


def verify_correctness(adaptdljobs, timeout=120):
    t = 0
    adaptdljobs = copy.deepcopy(adaptdljobs)
    while adaptdljobs and t < timeout:
        deletion = []
        for index, (error_type, job) in enumerate(adaptdljobs):
            new_job_status = custom_api.get_namespaced_custom_object_status(
                "adaptdl.petuum.com", "v1", NAMESPACE,
                "adaptdljobs", job["metadata"]["name"])
            job_status = new_job_status["status"]["phase"]
            if error_type == "":
                if job_status == "Succeeded":
                    deletion.append(index)
                elif job_status == "Failed":
                    return False
            else:
                if job_status == "Failed":
                    deletion.append(index)
                elif job_status == "Succeeded":
                    return False
        deletion.reverse()
        for index in deletion:
            del adaptdljobs[index]
        sleep(1)
        t += 1
    return (t < timeout)


result_template = {
    "apiVersion": "litmuschaos.io/v1alpha1",
    "kind": "ChaosResult",
    "metadata": {
        "name": os.environ["HOSTNAME"]
    },
    "status": {
        "experimentstatus": {
            "phase": None,
            "verdict": None
        }
    }
}


def create_result(passed):
    result = copy.deepcopy(result_template)
    result["spec"]["experimentstatus"]["phase"] = "Completed"
    result["spec"]["experimentstatus"]["verdict"] = \
        "Pass" if passed else "Fail"
    custom_api.create_namespaced_custom_object(
        "litmuschaos.io", "v1alpha1", NAMESPACE,
        "chaosresults", result)


def delete_jobs(names):
    for name in names:
        custom_api.delete_namespaced_custom_object(
            "adaptdl.petuum.com", "v1", NAMESPACE,
            "adaptdljobs", name)


def run_test(test_config):
    adaptdljobs = []
    for i in range(test_config["duration"]):
        if i % test_config["interval"] == 0:
            for j in range(test_config["adaptdljobs"]):
                error_type = ""
                if random.random() < test_config["prob_image_error"]:
                    error_type += "image"
                if random.random() < test_config["prob_name_error"]:
                    error_type += "name"
                adaptdljobs.append((
                    error_type,
                    create_adaptdljob(
                        test_config["yaml_path"], error_type)))
        sleep(1)
    names = [adaptdljob["metadata"]["name"]
             for _, adaptdljob in adaptdljobs]
    passed = verify_correctness(adaptdljobs)
    create_result(passed)
    delete_jobs(names)
    return passed


test_config = {
    "duration": args.duration,
    "interval": args.cycle_interval,
    "adaptdljobs": args.jobs_per_cycle,
    "prob_image_error": args.image_error_rate,
    "prob_name_error": args.name_error_rate,
    "yaml_path": args.job_path}

assert(run_test(test_config))
