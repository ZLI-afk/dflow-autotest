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
from .LAMMPS_OPs import (
    RelaxMakeLAMMPS,
    RelaxLAMMPS,
    RelaxPostLAMMPS,
    PropsMakeLAMMPS,
    PropsLAMMPS,
    PropsPostLAMMPS
)

from .lib.utils import identify_task

upload_packages.append(__file__)

class LAMMPSFlow(object):
    """
    Generate autotest workflow and automatically submit lammps jobs according to user input arguments.
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
        vasp_image_name = global_param.get("vasp_image_name", None)
        dpmd_image_name = global_param.get("dpmd_image_name", None)
        self.image_name = dpmd_image_name
        abacus_image_name = global_param.get("abacus_image_name", None)
        cpu_scass_type = global_param.get("cpu_scass_type", None)
        gpu_scass_type = global_param.get("gpu_scass_type", None)
        lammps_run_command = global_param.get("lammps_run_command", None)
        self.run_command = lammps_run_command
        vasp_run_command = global_param.get("vasp_run_command", None)
        abacus_run_command = global_param.get("abacus_run_command", None)

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
            image_pull_policy="IfNotPresent"
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
            image_pull_policy="IfNotPresent"
        )
        self.dispatcher_executor = dispatcher_executor_gpu

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
            template=PythonOPTemplate(RelaxMakeLAMMPS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input": upload_artifact(work_dir),
                       "param": upload_artifact(self.relax_param)},
        )
        self.relaxmake = relaxmake

        relax = PythonOPTemplate(RelaxLAMMPS,
                                       slices=Slices("{{item}}", input_artifact=["input_lammps"],
                                                     output_artifact=["output_lammps"]),
                                       image=self.image_name, command=["python3"])

        relaxcal = Step(
            name="RelaxLAMMPS-Cal",
            template=relax,
            artifacts={"input_lammps": relaxmake.outputs.artifacts["task_paths"]},
            parameters={"run_command": self.run_command},
            with_param=argo_range(relaxmake.outputs.parameters["njobs"]),
            key="LAMMPS-Cal-{{item}}",
            executor=self.dispatcher_executor
        )
        self.relaxcal = relaxcal

        relaxpost = Step(
            name="Relaxpost",
            template=PythonOPTemplate(RelaxPostLAMMPS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": relaxcal.outputs.artifacts["output_lammps"],
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
        else:
            propsmake = Step(
                name="Propsmake",
                template=PythonOPTemplate(PropsMakeLAMMPS, image=self.dpgen_image_name, command=["python3"]),
                artifacts={"input": upload_artifact(work_dir),
                           "param": upload_artifact(self.props_param)},
            )
        self.propsmake = propsmake

        props = PythonOPTemplate(PropsLAMMPS,
                                 slices=Slices("{{item}}", input_artifact=["input_lammps"],
                                               output_artifact=["output_lammps"]), image=self.image_name, command=["python3"])

        propscal = Step(
            name="PropsLAMMPS-Cal",
            template=props,
            artifacts={"input_lammps": propsmake.outputs.artifacts["task_paths"]},
            parameters={"run_command": self.run_command},
            with_param=argo_range(propsmake.outputs.parameters["njobs"]),
            key="LAMMPS-Cal-{{item}}",
            executor=self.dispatcher_executor
        )
        self.propscal = propscal

        propspost = Step(
            name="Propspost",
            template=PythonOPTemplate(PropsPostLAMMPS, image=self.dpgen_image_name, command=["python3"]),
            artifacts={"input_post": propscal.outputs.artifacts["output_lammps"],
                       "input_all": propsmake.outputs.artifacts["output"],
                       "param": upload_artifact(self.props_param)},
            parameters={"path": cwd}
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
