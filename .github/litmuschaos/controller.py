#  import argparse
import kubernetes
from yaml import load
from time import sleep
import random

kubernetes.config.load_incluster_config()
custom_api = kubernetes.client.CustomObjectsApi()
core_api = kubernetes.client.CoreV1Api()
NAMESPACE = "default"


def create_adaptdljob(yaml_path, error_type):
    with open(yaml_path, "r") as yaml_f:
        yaml = load(yaml_f)
    if "image" in error_type:
        del(yaml["spec"]["template"]["spec"]["containers"][0]["image"])
    if "name" in error_type:
        del(yaml["spec"]["template"]["spec"]["containers"][0]["name"])
    return custom_api.create_namespaced_custom_object(
        "adaptdl.petuum.com", "v1", NAMESPACE, "adaptdljobs", yaml)


def verify_correctness(adaptdljobs):
    while adaptdljobs:
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


def run_test(test_config):
    adaptdljobs = []
    for i in test_config["duration"]:
        if i % test_config["interval"] == 0:
            for j in test_config["adaptdljobs"]:
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
    verify_correctness(adaptdljobs)
