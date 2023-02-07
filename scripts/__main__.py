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
sys.path.append("..")
sys.path.append("../dflowprops")

#from dflowrelax.VASP_flow import main_vasp
#from dflowrelax.ABACUS_flow import main_abacus
#from dflowprops.LAMMPS_flow import main_lammps
from dflowprops.LAMMPS_flow import main_lammps


parser = argparse.ArgumentParser()
parser.add_argument("--vasp", help="Using VASP to perform relaxation",
                    action="store_true")
parser.add_argument("--abacus", help="Using ABACUS to perform relaxation",
                    action="store_true")
parser.add_argument("--lammps", help="Using LAMMPS to perform relaxation",
                    action="store_true")
args = parser.parse_args()

if args.vasp:
    pass
elif args.abacus:
    pass
elif args.lammps:
    main_lammps()

if __name__ == '__main__':
    main_lammps()
