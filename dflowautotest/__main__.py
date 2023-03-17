#!/usr/bin/env python

from dflow import config, s3_config
from dflow.plugins import bohrium
from dflow.plugins.bohrium import TiefblueClient
from monty.serialization import loadfn


config["host"] = "https://workflows.deepmodeling.com"
config["k8s_api_server"] = "https://workflows.deepmodeling.com"
username = loadfn("global.json").get("email",None)
bohrium.config["username"] = username
password = loadfn("global.json").get("password",None)
bohrium.config["password"] = password
program_id = loadfn("global.json").get("program_id",None)
bohrium.config["program_id"] = program_id
s3_config["repo_key"] = "oss-bohrium"
s3_config["storage_client"] = TiefblueClient()

import argparse
import sys
#sys.path.append("..")
#sys.path.append("../dflowautotest")

#from dflowrelax.VASP_flow import main_vasp
#from dflowrelax.ABACUS_flow import main_abacus
#from dflowautotest.LAMMPS_flow import main_lammps
from .LAMMPS_flow import main_lammps
from .VASP_flow import main_vasp
from .ABACUS_flow import main_abacus

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('files', type=str, nargs='+',
                        help='Input indicating json files')
    parser.add_argument("--vasp", help="Using VASP to perform autotest",
                        action="store_true")
    parser.add_argument("--abacus", help="Using ABACUS to perform autotest",
                        action="store_true")
    parser.add_argument("--lammps", help="Using LAMMPS to perform autotest",
                        action="store_true")
    parser.add_argument("--relax", help="Run relaxation before test properties",
                        action="store_true")
    args = parser.parse_args()
    return args

def main():
    args = parse_args()

    if args.vasp:
        main_vasp(args)
    elif args.abacus:
        main_abacus(args)
    elif args.lammps:
        main_lammps(args)

if __name__ == '__main__':
    main()
