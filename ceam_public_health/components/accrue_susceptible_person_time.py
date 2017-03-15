import operator

import pandas as pd, numpy as np

from ceam.framework.event import listens_for
from ceam.framework.population import uses_columns
from ceam.framework.values import modifies_value
from ceam import config

from ceam_inputs import get_age_bins


# TODO: Don't duplicate code! Get rid of the duplicate lines in the block below and setup
# get all gbd age groups
age_bins = get_age_bins()

# filter down all age groups to only the ones we care about
# FIXME: the age groups of interest will change for GBD 2016, since the 85-90 age group is in GBD 2016, but not 2015
age_bins = age_bins[(age_bins.age_group_id > 1) & (age_bins.age_group_id <= 21)]

age_bins.age_group_name = age_bins.age_group_name.str.lower()

age_bins.age_group_name = [x.strip().replace(' ', '_') for x in age_bins.age_group_name]

list_of_age_bins = []
susceptible_person_time_cols = []

year_start = config.getint('simulation_parameters', 'year_start')
year_end = config.getint('simulation_parameters', 'year_end')

for age_bin in pd.unique(age_bins.age_group_name.values):
    list_of_age_bins.append(age_bin)
    for year in range(year_start, year_end+1):
        susceptible_person_time_cols.append("susceptible_person_time_" + age_bin + "_in_year_{}".format(year))


class AccrueSusceptiblePersonTime():
    # TODO: Need to figure out how pass in all of the diseases
    def __init__(self, disease_col, susceptible_col):
        self.disease_col = disease_col
        self.susceptible_col = susceptible_col


    @listens_for('initialize_simulants')
    @uses_columns(susceptible_person_time_cols)
    def create_person_year_columns(self, event):
        length = len(event.index)
        for col in susceptible_person_time_cols:
            event.population_view.update(pd.DataFrame({col: np.zeros(length)}, index=event.index))


    def setup(self, builder):
        # get all gbd age groups
        age_bins = get_age_bins()

        # filter down all age groups to only the ones we care about
        # FIXME: the age groups of interest will change for GBD 2016, since the 85-90 age group is in GBD 2016, but not 2015
        age_bins = age_bins[(age_bins.age_group_id > 1) & (age_bins.age_group_id <= 21)]

        age_bins.age_group_name = age_bins.age_group_name.str.lower()

        age_bins.age_group_name = [x.strip().replace(' ', '_') for x in age_bins.age_group_name]

        self.dict_of_age_group_name_and_max_values = dict(zip(age_bins.age_group_name, age_bins.age_group_years_end))


    @listens_for('time_step', priority=9)
    @uses_columns(['diarrhea', 'age'] + susceptible_person_time_cols, 'alive')
    def count_time_steps_sim_has_diarrhea(self, event):
        pop = event.population_view.get(event.index)

        current_year = pd.Timestamp(event.time).year

        last_age_group_max = 0

        # sort self.dict_of_age_group_name_and_max_values by value (max age)
        sorted_dict = sorted(self.dict_of_age_group_name_and_max_values.items(), key=operator.itemgetter(1))
        for key, value in sorted_dict:
            # FIXME: Susceptible person time estimates are off unless end data falls exactly on a time step (so this is fine for the diarrhea model -- 1 day timesteps -- but may not be ok for other causes)
            pop.loc[(pop[self.disease_col] != self.susceptible_col) & (pop['age'] < value) & (pop['age'] >= last_age_group_max), 'susceptible_person_time_{k}_in_year_{c}'.format(k=key, c=current_year)] += config.getfloat('simulation_parameters', 'time_step')
            last_age_group_max = value


        event.population_view.update(pop)


    @modifies_value('metrics')
    @uses_columns(susceptible_person_time_cols)
    def metrics(self, index, metrics, population_view):
        population = population_view.get(index)

        for col in susceptible_person_time_cols:
            metrics[col] = population[col].sum()

        return metrics


# End.
