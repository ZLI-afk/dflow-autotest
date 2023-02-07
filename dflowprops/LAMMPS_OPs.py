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

import subprocess, os, shutil, glob, dpdata, pathlib
from pathlib import Path
from typing import List
from dflow.plugins.bohrium import BohriumContext, BohriumExecutor
from dpdata.periodic_table import Element
from monty.serialization import loadfn
from dflow.python import upload_packages
import shutil
upload_packages.append(__file__)

#from .lib.utils import return_prop_list

class PropsMakeLAMMPS(OP):
    """
    class for making calculation tasks
    """

    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output': Artifact(Path),
            'njobs': int,
            'jobs': Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd = os.getcwd()

        os.chdir(op_in["input"])
        work_d = os.getcwd()
        param_argv = 'param_prop.json'
        structures = loadfn(param_argv)["structures"]
        inter_parameter = loadfn(param_argv)["interaction"]
        parameter = loadfn(param_argv)["properties"]
        cmd = f'dpgen autotest make {param_argv}'
        subprocess.call(cmd, shell=True)

        conf_dirs = []
        for conf in structures:
            conf_dirs.extend(glob.glob(conf))
        conf_dirs.sort()

        from .lib.utils import return_prop_list
        prop_list = return_prop_list(parameter)
        task_list = []
        for ii in conf_dirs:
            conf_dir_global = os.path.join(work_d, ii)
            for jj in prop_list:
                task_list.append(os.path.join(conf_dir_global, jj))
            """
            for jj in prop_list:
                prop = os.path.join(conf_dir_global, jj)
                os.chdir(prop)
                prop_tasks = glob.glob(os.path.join(prop, 'task.*'))
                prop_tasks.sort()
                for kk in prop_tasks:
                    #bbb = kk
                    task_list.append(kk)
            """

        all_jobs = task_list
        njobs = len(all_jobs)
        jobs = []
        for job in all_jobs:
            jobs.append(pathlib.Path(job))

        os.chdir(cwd)
        op_out = OPIO({
            "output": op_in["input"],
            "njobs": njobs,
            "jobs": jobs
        })
        return op_out


class LAMMPS(OP):
    """
    class for LAMMPS calculation
    """

    def __init__(self, infomode=1):
        self.infomode = infomode

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_lammps': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_lammps': Artifact(Path, sub_path=False)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(op_in["input_lammps"])
        cmd = "for ii in task.*; do cd $ii; lmp -in in.lammps; cd ..; done"
        subprocess.call(cmd, shell=True)
        os.chdir(cwd)
        op_out = OPIO({
            "output_lammps": op_in["input_lammps"]
        })
        return op_out


class PropsPostLAMMPS(OP):
    """
    class for analyzing calculation results
    """

    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_post': Artifact(Path, sub_path=False),
            'path': str,
            'input_all': Artifact(Path, sub_path=False)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_all': Artifact(Path, sub_path=False)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        os.chdir(str(op_in['input_all'])+op_in['path'])
        shutil.copytree(str(op_in['input_post']) + op_in['path'], '../scripts/', dirs_exist_ok=True)

        param_argv = 'param_prop.json'
        cmd = f'dpgen autotest post {param_argv}'
        subprocess.call(cmd, shell=True)

        op_out = OPIO({
            'output_all': Path(str(op_in["input_all"])+op_in['path'])
        })
        return op_out
