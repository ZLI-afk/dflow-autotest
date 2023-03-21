from dflow import (
    Workflow,
    Step,
    argo_range,
    argo_len,
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
import os
from dflow.python import upload_packages

upload_packages.append(__file__)
from dflowautotest.VASP_OPs import (
    RelaxMakeVASP,
    RelaxPostVASP,
    PropsMakeVASP,
    PropsPostVASP
)
from dflowautotest.Flow import Flow

from fpop.vasp import PrepVasp, VaspInputs, RunVasp
from fpop.utils.step_config import (
    init_executor
)


class VASPFlow(Flow):
    """
    Generate autotest workflow and submit automatically for VASP Calculations.
    """
    def __init__(self, args):
        super().__init__(args)
        self.run_step_config_relax = {
            "executor": {
                "type": "dispatcher",
                "image_pull_policy": "IfNotPresent",
                "machine_dict": {
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
                        }
                    }
                }
            }
        }

        self.run_step_config_props = {
            "executor": {
                "type": "dispatcher",
                "image_pull_policy": "IfNotPresent",
                "machine_dict": {
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
                        }
                    }
                }
            }
        }

    def init_steps(self):
        cwd = os.getcwd()
        work_dir = cwd

        relaxmake = Step(
            name="Relaxmake",
            template=PythonOPTemplate(RelaxMakeVASP, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input": upload_artifact(work_dir),
                       "param": upload_artifact(self.relax_param)},
        )
        self.relaxmake = relaxmake

        relax = PythonOPTemplate(RunVasp,
                                 slices=Slices("{{item}}",
                                               input_parameter=["task_name"],
                                               input_artifact=["task_path"],
                                               output_artifact=["backward_dir"]),
                                 python_packages=self.upload_python_packages,
                                 image=self.vasp_image_name
                                 )

        relaxcal = Step(
            name="RelaxVASP-Cal",
            template=relax,
            parameters={
                "run_image_config": {"command": self.vasp_run_command},
                "task_name": relaxmake.outputs.parameters["task_names"],
                "backward_list": ["INCAR", "POSCAR", "OUTCAR", "CONTCAR"],
                "backward_dir_name": "relax_task"
            },
            artifacts={
                "task_path": relaxmake.outputs.artifacts["task_paths"]
            },
            with_param=argo_range(argo_len(relaxmake.outputs.parameters["task_names"])),
            key="RelaxVASP-Cal-{{item}}",
            executor=init_executor(self.run_step_config_relax.pop("executor")),
            **self.run_step_config_relax
        )
        self.relaxcal = relaxcal

        relaxpost = Step(
            name="Relaxpost",
            template=PythonOPTemplate(RelaxPostVASP, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": self.relaxcal.outputs.artifacts["backward_dir"], "input_all": self.relaxmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.relax_param)},
            parameters={"path": work_dir}
        )
        self.relaxpost = relaxpost

        if self.do_relax:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeVASP, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": relaxpost.outputs.artifacts["output_all"],
                           "param": upload_artifact(self.props_param)},
            )
        else:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeVASP, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": upload_artifact(work_dir),
                           "param": upload_artifact(self.props_param)},
            )
        self.propsmake = propsmake

        props = PythonOPTemplate(RunVasp,
                                 slices=Slices("{{item}}",
                                               input_parameter=["task_name"],
                                               input_artifact=["task_path"],
                                               output_artifact=["backward_dir"]),
                                 python_packages=self.upload_python_packages,
                                 image=self.vasp_image_name
                                 )

        propscal = Step(
            name="PropsVASP-Cal",
            template=props,
            parameters={
                "run_image_config": {"command": self.vasp_run_command},
                "task_name": propsmake.outputs.parameters["task_names"],
                "backward_list": ["INCAR", "POSCAR", "OUTCAR", "CONTCAR"]
            },
            artifacts={
                "task_path": propsmake.outputs.artifacts["task_paths"]
            },
            with_param=argo_range(argo_len(propsmake.outputs.parameters["task_names"])),
            key="PropsVASP-Cal-{{item}}",
            executor=init_executor(self.run_step_config_props.pop("executor")),
            **self.run_step_config_props
        )
        self.propscal = propscal

        propspost = Step(
            name="Propspost",
            template=PythonOPTemplate(PropsPostVASP, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": propscal.outputs.artifacts["backward_dir"], "input_all": self.propsmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.props_param)},
            parameters={"path": work_dir, "task_names": propsmake.outputs.parameters["task_names"]}
        )
        self.propspost = propspost
