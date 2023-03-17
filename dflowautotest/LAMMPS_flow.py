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
from .LAMMPS_OPs import (
    RelaxMakeLAMMPS,
    RelaxLAMMPS,
    RelaxPostLAMMPS,
    PropsMakeLAMMPS,
    PropsLAMMPS,
    PropsPostLAMMPS
)
from .lib.utils import determine_task

upload_packages.append(__file__)

class FlowGenerator(object):
    def __int__(self, args):
        # initiate params defined in global.json
        global_param = loadfn("global.json")
        self.global_param = global_param
        work_dir = global_param.get("work_dir", None)
        email = global_param.get("email", None)
        password = global_param.get("password", None)
        program_id = global_param.get("program_id", None)
        self.dpgen_image_name = global_param.get("dpgen_image_name", None)
        self.vasp_image_name = global_param.get("vasp_image_name", None)
        self.dpmd_image_name = global_param.get("dpmd_image_name", None)
        self.abacus_image_name = global_param.get("abacus_image_name", None)
        cpu_scass_type = global_param.get("cpu_scass_type", None)
        gpu_scass_type = global_param.get("gpu_scass_type", None)

        self.dispatcher_executor_cpu = DispatcherExecutor(
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

        self.dispatcher_executor_gpu = DispatcherExecutor(
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
        # determine type of flow and input parameter file
        num_args = len(args.files)
        if num_args == 1:
            self.do_relax = False
            task_type = determine_task(args.files[0])
            if task_type == 'relax':
                self.relax_param = args.files[0]
                self.props_param = None
            elif task_type == 'props':
                self.relax_param = None
                self.props_param = args.files[0]
        elif num_args == 2:
            self.do_relax = True
            file1_type = determine_task(args.files[0])
            file2_type = determine_task(args.files[1])
            if not file1_type == file2_type:
                if file1_type == 'relax':
                    self.relax_param = args.files[0]
                    self.props_param = args.files[1]
                else:
                    self.relax_param = args.files[1]
                    self.props_param = args.files[0]
            else:
                raise (RuntimeError('same type of input json files'))
        else:
            raise (ValueError('only two input args are allowed'))

    def init_steps(self):
        cwd = os.getcwd()
        work_dir = cwd

        relaxmake = Step(
            name="Relaxmake",
            template=PythonOPTemplate(RelaxMakeLAMMPS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input": upload_artifact(work_dir),
                       "param": upload_artifact(self.relax_param)},
        )
        self.relaxmake = relaxmake

        relaxlammps = PythonOPTemplate(RelaxLAMMPS,
                                       slices=Slices("{{item}}", input_artifact=["input_lammps"],
                                                     output_artifact=["output_lammps"]),
                                       image=self.dpmd_image_name, command=["python3"])

        relaxlammps_cal = Step(
            name="RelaxLAMMPS-Cal",
            template=relaxlammps,
            artifacts={"input_lammps": relaxmake.outputs.artifacts["jobs"]},
            with_param=argo_range(relaxmake.outputs.parameters["njobs"]),
            key="LAMMPS-Cal-{{item}}",
            executor=self.dispatcher_executor_gpu
        )
        self.relaxlammps_cal = relaxlammps_cal

        relaxpost = Step(
            name="Relaxpost",
            template=PythonOPTemplate(RelaxPostLAMMPS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": relaxlammps_cal.outputs.artifacts["output_lammps"],
                       "input_all": relaxmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.relax_param)},
            parameters={"path": cwd}
        )
        self.relaxpost = relaxpost

        if self.do_relax:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeLAMMPS, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": relaxpost.outputs.artifacts["output_all"],
                           "param": upload_artifact(self.props_param)},
            )
            self.propsmake = propsmake
        else:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeLAMMPS, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": upload_artifact(work_dir),
                           "param": upload_artifact(self.props_param)},
            )
            self.propsmake = propsmake

        propslammps = PythonOPTemplate(PropsLAMMPS,
                                       slices=Slices("{{item}}", input_artifact=["input_lammps"],
                                                     output_artifact=["output_lammps"]), image=self.dpmd_image_name, command=["python3"])

        propslammps_cal = Step(
            name="PropsLAMMPS-Cal",
            template=propslammps,
            artifacts={"input_lammps": propsmake.outputs.artifacts["jobs"]},
            with_param=argo_range(propsmake.outputs.parameters["njobs"]),
            key="LAMMPS-Cal-{{item}}",
            executor=self.dispatcher_executor_gpu
        )
        self.propslammps_cal = propslammps_cal

        propspost = Step(
            name="Propspost",
            template=PythonOPTemplate(PropsPostLAMMPS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": propslammps_cal.outputs.artifacts["output_lammps"],
                       "input_all": propsmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.props_param)},
            parameters={"path": cwd}
        )
        self.propspost = propspost

    def make_relax_flow(self):
        wf = Workflow(name='Relaxation')
        wf.add(self.relaxmake)
        wf.add(self.relaxlammps_cal)
        wf.add(self.relaxpost)
        wf.submit()

        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)
        assert (wf.query_status() == 'Succeeded')
        step = wf.query_step(name="Relaxpost")[0]
        download_artifact(step.outputs.artifacts["output_confs"])

    def make_prop_flow(self):
        wf = Workflow(name='Properties')
        wf.add(self.propsmake)
        wf.add(self.propslammps_cal)
        wf.add(self.propspost)
        wf.submit()

        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)
        assert (wf.query_status() == 'Succeeded')
        step = wf.query_step(name="Propspost")[0]
        download_artifact(step.outputs.artifacts["output_confs"])

    def make_relax_prop_flow(self):
        wf = Workflow(name='Relaxation&Properties')
        wf.add(self.relaxmake)
        wf.add(self.relaxlammps_cal)
        wf.add(self.relaxpost)
        wf.add(self.propsmake)
        wf.add(self.propslammps_cal)
        wf.add(self.propspost)
        wf.submit()

        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)
        assert (wf.query_status() == 'Succeeded')
        step = wf.query_step(name="Propspost")[0]
        download_artifact(step.outputs.artifacts["output_confs"])


def main_lammps(args):
    flow = FlowGenerator(args)
    flow.init_steps()
    if flow.do_relax == True:
        flow.make_relax_prop_flow()
    elif flow.props_param == None:
        flow.make_relax_flow()
    else:
        flow.make_prop_flow()
