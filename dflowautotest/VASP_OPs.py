from dflow.python import (
    PythonOPTemplate,
    OP,
    OPIO,
    OPIOSign,
    Artifact,
    Slices,
    upload_packages
)

import os, glob, dpdata, pathlib
from pathlib import Path
from typing import List
from monty.serialization import loadfn

try:
    from dpgen.auto_test.common_equi import (make_equi, post_equi)
except:
    pass

try:
    from dpgen.auto_test.common_prop import (make_property, post_property)
except:
    pass

import shutil
upload_packages.append(__file__)

from dflowautotest.lib.utils import return_prop_list


class RelaxMakeVASP(OP):
    """
    class for making calculation tasks
    """

    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input': Artifact(Path),
            'param': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output': Artifact(Path),
            'task_names': List[str],
            'task_paths': Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd = os.getcwd()

        os.chdir(op_in["input"])
        work_d = os.getcwd()
        param_argv = op_in["param"]
        structures = loadfn(param_argv)["structures"]
        inter_parameter = loadfn(param_argv)["interaction"]
        parameter = loadfn(param_argv)["relaxation"]

        make_equi(structures, inter_parameter, parameter)

        conf_dirs = []
        for conf in structures:
            conf_dirs.extend(glob.glob(conf))
        conf_dirs.sort()

        task_list = []
        task_list_str = []
        for ii in conf_dirs:
            conf_dir_global = os.path.join(work_d, ii)
            task_list.append(os.path.join(conf_dir_global, 'relaxation/relax_task'))
            task_list_str.append(os.path.join(ii, 'relaxation'))

        all_jobs = task_list
        jobs = []
        for job in all_jobs:
            jobs.append(pathlib.Path(job))

        os.chdir(cwd)
        op_out = OPIO({
            "output": op_in["input"],
            "task_names": task_list_str,
            "task_paths": jobs
        })
        return op_out


class RelaxPostVASP(OP):
    """
    class for analyzing calculation results
    """

    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_post': Artifact(Path, sub_path=False),
            'input_all': Artifact(Path, sub_path=False),
            'param': Artifact(Path),
            'path': str
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_post': Artifact(Path, sub_path=False),
            'output_all': Artifact(Path, sub_path=False)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(str(op_in['input_all']) + op_in['path'])
        shutil.copytree(str(op_in['input_post']), './', dirs_exist_ok=True)

        param_argv = op_in['param']
        post_equi(loadfn(param_argv)["structures"], loadfn(param_argv)["interaction"])

        os.chdir(cwd)
        shutil.copytree(str(op_in['input_all']) + op_in['path'] + '/confs', './confs', dirs_exist_ok=True)

        op_out = OPIO({
            'output_post': Path("./confs"),
            'output_all': Path(str(op_in['input_all']) + op_in['path'])
        })
        return op_out


class PropsMakeVASP(OP):
    """
    class for making calculation tasks
    """

    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input': Artifact(Path),
            'param': Artifact(Path)
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output': Artifact(Path),
            'task_names': List[str],
            'task_paths': Artifact(List[Path])
        })

    @OP.exec_sign_check
    def execute(
            self,
            op_in: OPIO,
    ) -> OPIO:
        cwd = os.getcwd()

        os.chdir(op_in["input"])
        work_d = os.getcwd()
        param_argv = op_in["param"]
        structures = loadfn(param_argv)["structures"]
        inter_parameter = loadfn(param_argv)["interaction"]
        parameter = loadfn(param_argv)["properties"]
        make_property(structures, inter_parameter, parameter)

        conf_dirs = []
        for conf in structures:
            conf_dirs.extend(glob.glob(conf))
        conf_dirs.sort()

        prop_list = return_prop_list(parameter)
        task_list = []
        task_list_str = []
        for ii in conf_dirs:
            conf_dir_global = os.path.join(work_d, ii)
            for jj in prop_list:
                prop = os.path.join(conf_dir_global, jj)
                prop_tasks = glob.glob(os.path.join(prop, 'task.*'))
                prop_tasks.sort()
                task_list.extend(prop_tasks)
                prop_tasks_str = glob.glob(os.path.join(ii, jj, 'task.*'))
                prop_tasks_str.sort()
                task_list_str.extend(prop_tasks_str)

        all_jobs = task_list
        jobs = []
        for job in all_jobs:
            jobs.append(pathlib.Path(job))

        os.chdir(cwd)
        op_out = OPIO({
            "output": op_in["input"],
            "task_names": task_list_str,
            "task_paths": jobs
        })
        return op_out


class PropsPostVASP(OP):
    """
    class for analyzing calculation results
    """

    def __init__(self):
        pass

    @classmethod
    def get_input_sign(cls):
        return OPIOSign({
            'input_post': Artifact(Path, sub_path=False),
            'input_all': Artifact(Path, sub_path=False),
            'param': Artifact(Path),
            'path': str,
            'task_names': List[str]
        })

    @classmethod
    def get_output_sign(cls):
        return OPIOSign({
            'output_post': Artifact(Path, sub_path=False)
        })

    @OP.exec_sign_check
    def execute(self, op_in: OPIO) -> OPIO:
        cwd = os.getcwd()
        os.chdir(str(op_in['input_post']))
        for ii in op_in['task_names']:
            shutil.copytree(os.path.join(ii, "backward_dir"), ii, dirs_exist_ok=True)
            shutil.rmtree(os.path.join(ii, "backward_dir"))

        os.chdir(str(op_in['input_all']) + op_in['path'])
        shutil.copytree(str(op_in['input_post']), './', dirs_exist_ok=True)

        param_argv = op_in['param']
        post_property(loadfn(param_argv)["structures"], loadfn(param_argv)["interaction"], loadfn(param_argv)["properties"])

        os.chdir(cwd)
        shutil.copytree(str(op_in['input_all']) + op_in['path'] + '/confs', './confs', dirs_exist_ok=True)

        op_out = OPIO({
            'output_post': Path("./confs")
        })
        return op_out
