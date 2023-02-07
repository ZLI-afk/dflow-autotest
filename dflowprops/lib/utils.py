#!/usr/bin/env python3

def return_prop_list(parameters):
    prop_list = []
    for ii in parameters:
        if ii.get('skip', False):
            continue
        if 'init_from_suffix' and 'output_suffix' in ii:
            #do_refine = True
            suffix = ii['output_suffix']
        elif 'reproduce' in ii and ii['reproduce']:
            #do_refine = False
            suffix = 'reprod'
        else:
            #do_refine = False
            suffix = '00'
        prop_list.append(ii['type'] + '_' + suffix)
    return prop_list
