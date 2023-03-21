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
import time, os
from dflow.plugins.dispatcher import DispatcherExecutor
from monty.serialization import loadfn
from dflow.python import upload_packages
from dflowautotest.ABACUS_OPs import (
    RelaxMakeABACUS,
    RelaxABACUS,
    RelaxPostABACUS,
    PropsMakeABACUS,
    PropsABACUS,
    PropsPostABACUS
)
from dflowautotest.Flow import Flow

upload_packages.append(__file__)


class ABACUSFlow(Flow):
    """
    Generate autotest workflow and automatically submit abacus jobs according to user input arguments.
    """
    def __init__(self, args):
        super().__init__(args)
        dispatcher_executor_cpu = DispatcherExecutor(
            machine_dict={
                "batch_type": "Bohrium",
                "context_type": "Bohrium",
                "remote_profile": {
                    "email": self.email,
                    "password": self.password,
                    "program_id": self.program_id,
                    "input_data": {
                        "job_type": "container",
                        "platform": "ali",
                        "scass_type": self.cpu_scass_type,
                    },
                },
            },
            image_pull_policy="IfNotPresent"
        )

        dispatcher_executor_gpu = DispatcherExecutor(
            machine_dict={
                "batch_type": "Bohrium",
                "context_type": "Bohrium",
                "remote_profile": {
                    "email": self.email,
                    "password": self.password,
                    "program_id": self.program_id,
                    "input_data": {
                        "job_type": "container",
                        "platform": "ali",
                        "scass_type": self.gpu_scass_type,
                    },
                },
            },
            image_pull_policy="IfNotPresent"
        )
        self.dispatcher_executor = dispatcher_executor_cpu

    def init_steps(self):
        cwd = os.getcwd()
        work_dir = cwd

        relaxmake = Step(
            name="Relaxmake",
            template=PythonOPTemplate(RelaxMakeABACUS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input": upload_artifact(work_dir),
                       "param": upload_artifact(self.relax_param)},
        )
        self.relaxmake = relaxmake

        relax = PythonOPTemplate(RelaxABACUS,
                                       slices=Slices("{{item}}", input_artifact=["input_abacus"],
                                                     output_artifact=["output_abacus"]),
                                       image=self.abacus_image_name, command=["python3"])

        relaxcal = Step(
            name="RelaxABACUS-Cal",
            template=relax,
            artifacts={"input_abacus": relaxmake.outputs.artifacts["task_paths"]},
            parameters={"run_command": self.abacus_run_command},
            with_param=argo_range(relaxmake.outputs.parameters["njobs"]),
            key="ABACUS-Cal-{{item}}",
            executor=self.dispatcher_executor
        )
        self.relaxcal = relaxcal

        relaxpost = Step(
            name="Relaxpost",
            template=PythonOPTemplate(RelaxPostABACUS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": relaxcal.outputs.artifacts["output_abacus"],
                       "input_all": relaxmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.relax_param)},
            parameters={"path": cwd}
        )
        self.relaxpost = relaxpost

        if self.do_relax:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeABACUS, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": relaxpost.outputs.artifacts["output_all"],
                           "param": upload_artifact(self.props_param)},
            )
            self.propsmake = propsmake
        else:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeABACUS, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": upload_artifact(work_dir),
                           "param": upload_artifact(self.props_param)},
            )
            self.propsmake = propsmake

        props = PythonOPTemplate(PropsABACUS,
                                 slices=Slices("{{item}}", input_artifact=["input_abacus"],
                                               output_artifact=["output_abacus"]), image=self.abacus_image_name, command=["python3"])

        propscal = Step(
            name="PropsABACUS-Cal",
            template=props,
            artifacts={"input_abacus": propsmake.outputs.artifacts["task_paths"]},
            parameters={"run_command": self.abacus_run_command},
            with_param=argo_range(propsmake.outputs.parameters["njobs"]),
            key="ABACUS-Cal-{{item}}",
            executor=self.dispatcher_executor
        )
        self.propscal = propscal

        propspost = Step(
            name="Propspost",
            template=PythonOPTemplate(PropsPostABACUS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": propscal.outputs.artifacts["output_abacus"],
                       "input_all": propsmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.props_param)},
            parameters={"path": cwd}
        )
        self.propspost = propspost
