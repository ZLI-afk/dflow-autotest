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
import time, os
from monty.serialization import loadfn
from dflow.python import upload_packages

upload_packages.append(__file__)
from dflowautotest.lib.utils import identify_task
from dflowautotest.VASP_OPs import RelaxMakeVASP, RelaxPostVASP, PropsMakeVASP, PropsPostVASP

from fpop.vasp import PrepVasp, VaspInputs, RunVasp
from fpop.utils.step_config import (
    init_executor
)


class VASPFlow(object):
    """
    Generate autotest workflow and submit automatically for VASP Calculations.
    """

    def __init__(self, args):
        # initiate params defined in global.json
        global_param = loadfn("global.json")
        self.global_param = global_param
        work_dir = global_param.get("work_dir", None)
        email = global_param.get("email", None)
        password = global_param.get("password", None)
        program_id = global_param.get("program_id", None)
        self.dpgen_image_name = global_param.get("dpgen_image_name", None)
        self.vasp_image_name = global_param.get("vasp_image_name", None)
        dpmd_image_name = global_param.get("dpmd_image_name", None)
        abacus_image_name = global_param.get("abacus_image_name", None)
        cpu_scass_type = global_param.get("cpu_scass_type", None)
        gpu_scass_type = global_param.get("gpu_scass_type", None)
        lammps_run_command = global_param.get("lammps_run_command", None)
        self.vasp_run_command = global_param.get("vasp_run_command", None)
        abacus_run_command = global_param.get("abacus_run_command", None)
        self.upload_python_packages = global_param.get("upload_python_packages", None)

        self.run_step_config_relax = {
            "executor": {
                "type": "dispatcher",
                "image_pull_policy": "IfNotPresent",
                "machine_dict": {
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
                        "email": email,
                        "password": password,
                        "program_id": program_id,
                        "input_data": {
                            "job_type": "container",
                            "platform": "ali",
                            "scass_type": cpu_scass_type,
                        }
                    }
                }
            }
        }

        # identify type of flow and input parameter file
        num_args = len(args.files)
        if num_args == 1:
            self.do_relax = False
            task_type = identify_task(args.files[0])
            if task_type == 'relax':
                self.relax_param = args.files[0]
                self.props_param = None
            elif task_type == 'props':
                self.relax_param = None
                self.props_param = args.files[0]
        elif num_args == 2:
            self.do_relax = True
            file1_type = identify_task(args.files[0])
            file2_type = identify_task(args.files[1])
            if not file1_type == file2_type:
                if file1_type == 'relax':
                    self.relax_param = args.files[0]
                    self.props_param = args.files[1]
                else:
                    self.relax_param = args.files[1]
                    self.props_param = args.files[0]
            else:
                raise RuntimeError('Same type of input json files')
        else:
            raise ValueError('A maximum of two input arguments is allowed')

        if self.do_relax:
            self.flow_type = 'joint'
        elif not self.props_param:
            self.flow_type = 'relax'
        else:
            self.flow_type = 'props'

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

    @staticmethod
    def assertion(wf, task_type):
        while wf.query_status() in ["Pending", "Running"]:
            time.sleep(4)
        assert (wf.query_status() == 'Succeeded')
        step = wf.query_step(name=f"{task_type}post")[0]
        download_artifact(step.outputs.artifacts["output_post"])

    def generate_flow(self):
        if self.flow_type == 'relax':
            wf = Workflow(name='relaxation')
            wf.add(self.relaxmake)
            wf.add(self.relaxcal)
            wf.add(self.relaxpost)
            wf.submit()
            self.assertion(wf, 'Relax')

        elif self.flow_type == 'props':
            wf = Workflow(name='properties')
            wf.add(self.propsmake)
            wf.add(self.propscal)
            wf.add(self.propspost)
            wf.submit()
            self.assertion(wf, 'Props')

        elif self.flow_type == 'joint':
            wf = Workflow(name='relax-props')
            wf.add(self.relaxmake)
            wf.add(self.relaxcal)
            wf.add(self.relaxpost)
            wf.add(self.propsmake)
            wf.add(self.propscal)
            wf.add(self.propspost)
            wf.submit()
            self.assertion(wf, 'Props')
