import time
from abc import ABC, abstractmethod
from monty.serialization import loadfn

from dflow import download_artifact, Workflow

from dflowautotest.lib.utils import identify_task


class Flow(ABC):
    def __init__(self, args):
        # initiate params defined in global.json
        global_param = loadfn("global.json")
        self.args = args
        self.global_param = global_param
        self.work_dir = global_param.get("work_dir", None)
        self.email = global_param.get("email", None)
        self.password = global_param.get("password", None)
        self.program_id = global_param.get("program_id", None)
        self.dpgen_image_name = global_param.get("dpgen_image_name", None)
        self.vasp_image_name = global_param.get("vasp_image_name", None)
        self.dpmd_image_name = global_param.get("dpmd_image_name", None)
        self.abacus_image_name = global_param.get("abacus_image_name", None)
        self.cpu_scass_type = global_param.get("cpu_scass_type", None)
        self.gpu_scass_type = global_param.get("gpu_scass_type", None)
        self.lammps_run_command = global_param.get("lammps_run_command", None)
        self.vasp_run_command = global_param.get("vasp_run_command", None)
        self.abacus_run_command = global_param.get("abacus_run_command", None)
        self.upload_python_packages = global_param.get("upload_python_packages", None)

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

    @abstractmethod
    def init_steps(self):
        pass

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



