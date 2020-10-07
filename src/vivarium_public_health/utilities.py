"""
=========
Utilities
=========

This module contains utility classes and functions for use across
vivarium_public_health components.

"""
from typing import Union

import glob
import numpy as np
import os
import pandas as pd
from scipy.sparse import coo_matrix
import scipy
import yaml

def read_config_file(filename=r'../config/model_specification.yaml'):
    """read a config file"""
    with open(filename) as inp_file_io:
        inp_file = yaml.load(inp_file_io, Loader=yaml.FullLoader)
    return inp_file

def csv2sparse(path2csv="../persistant_data/od_matrices/*.csv"):

    list_of_files = glob.glob(path2csv)
    
    for i, fi in enumerate(list_of_files):
        print(f"Processing: {fi}")
        od = pd.read_csv(fi).values

        if i == 0:
            od_map = {}
            for i, item in enumerate(od[:, 0]):
                od_map[item] = i
            od_map_pd = pd.DataFrame.from_dict(od_map, orient="index", columns=["indices"]) 
            od_map_pd.to_csv(os.path.join(os.path.dirname(fi), os.pardir, "MSOA_to_OD_index.csv"))
        
        od_val = od[:, 1:]
        od_val = od_val.astype(np.float)
        od_val_sparse = coo_matrix(od_val)
        scipy.sparse.save_npz(fi.split(".csv")[0] + ".npz", od_val_sparse)

class EntityString(str):
    """Convenience class for representing entities as strings."""

    def __init__(self, entity):
        super().__init__()
        self._type, self._name = self.split_entity()

    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self._name

    def split_entity(self):
        split = self.split('.')
        if len(split) != 2:
            raise ValueError(f'You must specify the entity as "entity_type.entity". You specified {self}.')
        return split[0], split[1]


class TargetString(str):
    """Convenience class for representing risk targets as strings."""

    def __init__(self, target):
        super().__init__()
        self._type, self._name, self._measure = self.split_target()

    @property
    def type(self):
        return self._type

    @property
    def name(self):
        return self._name

    @property
    def measure(self):
        return self._measure

    def split_target(self):
        split = self.split('.')
        if len(split) != 3:
            raise ValueError(
                f'You must specify the target as "affected_entity_type.affected_entity_name.affected_measure".'
                f'You specified {self}.')
        return split[0], split[1], split[2]


DAYS_PER_YEAR = 365.25
DAYS_PER_MONTH = DAYS_PER_YEAR / 12


def to_time_delta(span_in_days: Union[int, float, str]):
    span_in_days = float(span_in_days)
    days, remainder = span_in_days // 1, span_in_days % 1
    hours, remainder = (24 * remainder) // 24, (24 * remainder) % 24
    minutes = (60 * remainder) // 60
    return pd.Timedelta(days=days, hours=hours, minutes=minutes)


def to_years(time: pd.Timedelta) -> float:
    """Converts a time delta to a float for years."""
    return time / pd.Timedelta(days=DAYS_PER_YEAR)

def map_missing_LAD(LAD_names):
    '''Maps LAD names to the ones needed existing in the rates'''

    missing_LAD = ['E06000052', 'E06000053', 'E06000057', 'E07000240', 'E07000241',
       'E07000242', 'E07000243', 'E08000037', 'E09000001', 'E09000033']

    map_dict = {'E09000001': 'E09000001+E09000033', 'E09000033': 'E09000001+E09000033',
                'E06000052': 'E06000052+E06000053', 'E06000053': 'E06000052+E06000053',
                'E06000057': 'E06000052+E06000053', 'E07000240': 'E06000052+E06000053',
                'E07000241': 'E06000052+E06000053', 'E07000242': 'E06000052+E06000053',
                'E07000243': 'E06000052+E06000053', 'E08000037': 'E06000052+E06000053'}

    for index, value in enumerate(LAD_names):

        if value in missing_LAD:
            LAD_names[index] = map_dict[value]

    return LAD_names