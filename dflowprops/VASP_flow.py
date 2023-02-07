import json, pathlib
from typing import List
from dflow import (
    Workflow,
    Step,
    argo_range,
    SlurmRemoteExecutor,
    upload_artifact,
    download_artifact,
    InputArtifact,
    OutputArtifact,
    ShellOPTemplate
)
from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices,
    upload_packages
)
import time
import subprocess, os, shutil, glob
from pathlib import Path
from typing import List
from dflow.plugins.dispatcher import DispatcherExecutor
from monty.serialization import loadfn
from monty.serialization import loadfn
from dflow.python import upload_packages

upload_packages.append(__file__)


def main_vasp():
    from .VASP_OPs import PropsMakeVASP, VASP, PropsPostVASP
    global_param = loadfn("global.json")
    work_dir = global_param.get("work_dir", None)
    email = global_param.get("email", None)
    password = global_param.get("password", None)
    program_id = global_param.get("program_id", None)
    dpgen_image_name = global_param.get("dpgen_image_name", None)
    vasp_image_name = global_param.get("vasp_image_name", None)
    abacus_image_name = global_param.get("abacus_image_name", None)
    cpu_scass_type = global_param.get("cpu_scass_type", None)
    gpu_scass_type = global_param.get("gpu_scass_type", None)
    vasp_run_command = global_param.get("vasp_run_command", None)

    dispatcher_executor_cpu = DispatcherExecutor(
        machine_dict={
            "batch_type": "Bohrium",
            "context_type": "Bohrium",
            "remote_profile": {
                "email": email,
                "password": password,
                "program_id": program_id,
                "input_data": {
                    "job_type": "container",
                    "platform": "ali",
                    "scass_type": cpu_scass_type,
                },
            },
        },
    )

    dispatcher_executor_gpu = DispatcherExecutor(
        machine_dict={
            "batch_type": "Bohrium",
            "context_type": "Bohrium",
            "remote_profile": {
                "email": email,
                "password": password,
                "program_id": program_id,
                "input_data": {
                    "job_type": "container",
                    "platform": "ali",
                    "scass_type": gpu_scass_type,
                },
            },
        },
    )

    cwd = os.getcwd()
    work_dir = cwd
    wf = Workflow(name="relax")

    Propsmake = Step(
        name="Propsmake",
        template=PythonOPTemplate(PropsMakeVASP, image=dpgen_image_name, command=["python3"]),
        artifacts={"input": upload_artifact(work_dir)},
    )
    wf.add(Propsmake)

    vasp = PythonOPTemplate(VASP,
                            slices=Slices("{{item}}", input_artifact=["input_vasp"], output_artifact=["output_vasp"]),
                            image=vasp_image_name, command=["python3"])
    vasp_cal = Step(
        name="VASP-Cal",
        template=vasp,
        artifacts={"input_vasp": Propsmake.outputs.artifacts["jobs"]},
        parameters={"run_command": vasp_run_command},
        with_param=argo_range(Propsmake.outputs.parameters["njobs"]),
        key="VASP-Cal-{{item}}",
        executor=dispatcher_executor_cpu
    )
    wf.add(vasp_cal)

    Propspost = Step(
        name="Propspost",
        template=PythonOPTemplate(PropsPostVASP, image=dpgen_image_name, command=["python3"]),
        artifacts={"input_post": vasp_cal.outputs.artifacts["output_vasp"], "input_all": Propsmake.outputs.artifacts["output"]},
        parameters={"path": cwd}
    )
    wf.add(Propspost)

    wf.submit()

    while wf.query_status() in ["Pending", "Running"]:
        time.sleep(4)
    assert (wf.query_status() == 'Succeeded')
    step = wf.query_step(name="Propspost")[0]
    download_artifact(step.outputs.artifacts["output_all"])
