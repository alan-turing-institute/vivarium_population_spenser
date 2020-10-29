"""
==========================
The Core Immigration Model
==========================

Currently, we have a deterministic immigration component in which:
- the total number of immigrants is read from a file
- the characteristics of the immigrants are sampled from the migration rate file

"""
import pandas as pd
import numpy as np
from vivarium_population_spenser import utilities


class ImmigrationDeterministic:

    @property
    def name(self):
        return "deterministic_immigration"

    def setup(self, builder):
        self.fractional_new_immigrations = 0
        # read rates and total number of immigrants
        self.asfr_data_immigration = builder.data.load("cause.all_causes.cause_specific_immigration_rate") 
        self.simulants_per_year = builder.data.load("cause.all_causes.cause_specific_total_immigrants_per_year") 
        self.immigration_to_MSOA = builder.data.load("cause.all_causes.immigration_to_MSOA")

        self.simulant_creator = builder.population.get_simulant_creator()
        self.population_view = builder.population.get_view(['immigrated', 'sex', 'ethnicity', 'location', 'age','MSOA'])
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=["immigrated"])
        builder.event.register_listener('time_step', self.on_time_step)

    def on_initialize_simulants(self, pop_data):
        if pop_data.user_data['sim_state'] == 'time_step_imm':
            pop_update = pd.DataFrame({'immigrated': 'Yes'},
                                    index=pop_data.index)
        else:
            pop_update = pd.DataFrame({'immigrated': 'no_immigration'},
                                    index=pop_data.index)


        self.population_view.update(pop_update)

    def on_time_step(self, event):
        """Adds a set number of simulants to the population each time step.

        Parameters
        ----------
        event
            The event that triggered the function call.
        """
        # Assume immigrants are uniformly distributed throughout the year.
        step_size = utilities.to_years(event.step_size)
        simulants_to_add = self.simulants_per_year*step_size + self.fractional_new_immigrations

        self.fractional_new_immigrations = simulants_to_add % 1
        simulants_to_add = int(simulants_to_add)

        if simulants_to_add > 0:
            self.simulant_creator(simulants_to_add,
                                  population_configuration={
                                      'age_start': 0,
                                      'age_end': 100,
                                      'sim_state': 'time_step_imm',
                                      'immigrated': "Yes"
                                  })
        
        # XXX make sure this does not conflict with fertility XXX
        new_residents = self.population_view.get(event.index+simulants_to_add,query='sex == "nan" and immigrated != "no_immigration"').copy()
        
        if len(new_residents) > 0:
            # sample residents using the immigration rates
            sample_resident = self.asfr_data_immigration.sample(len(new_residents), weights="mean_value", replace=True)
            new_residents["sex"] = sample_resident["sex"].values.astype(float)
            new_residents["ethnicity"] = sample_resident["ethnicity"].values
            new_residents["location"] = sample_resident["location"].values
            new_residents["age"] = sample_resident["age_start"].values.astype(float)
            new_residents["immigrated"] = "Yes"

            new_residents['MSOA'] = self.assign_MSOA(new_residents)

            self.population_view.update(new_residents[['immigrated', 'location', 'ethnicity', 'sex', 'age','MSOA']])

    def assign_MSOA(self,new_residents):
        ''' Based on the characteristic individuals of the new residents, get the relevant
         assign new MSOA and save the old ones in a new field
         '''
        int_migration_matrix_rate, int_migration_matrix_names = self.get_immigration_MSOA_rates(new_residents)

        # Add a random seed for numpy
        np.random.seed(64)

        # sample the rates for each individual and get the new wards.
        c = int_migration_matrix_rate.cumsum(axis=1)
        u = np.random.rand(len(c), 1)
        # get the new MSA
        MSOA_choices = (u < c).argmax(axis=1)

        # from the MSOA index get the new MSOA and LAD location name
        MSOA_choices_name = int_migration_matrix_names[MSOA_choices]

        return (MSOA_choices_name)

    def get_OD_matrix_age_gender(self,new_residents):

        cut_bins = [-1, 5, 16, 20, 25, 35, 50, 65, 75, 200]

        cut_labels = ["0_4", "5_15", "16_19", "20_24", "25_34", "35_49", "50_64", "65_74", "75plus"]
        new_residents.loc[:, "age_bucket"] = pd.cut(new_residents['age'], bins=cut_bins, labels=cut_labels)
        # XXX recheck the sex_map
        new_residents.loc[:, "sex_map"] = new_residents["sex"].map({1: 'M', 2: 'F'})
        new_residents["MSOA_values"] = new_residents["sex_map"].astype(str) + "_" + new_residents["age_bucket"].astype(str)

        return new_residents

    def get_immigration_MSOA_rates(self,new_residents):

        new_residents = self.get_OD_matrix_age_gender(new_residents)
        LAD_name = np.unique(new_residents['location'])

        if len(LAD_name)!=1:
            raise RuntimeError('The immigration module only works on the individual LAD level')

        immigration_values = self.immigration_to_MSOA[self.immigration_to_MSOA['LAD.Code'].isin(LAD_name)][new_residents['MSOA_values']]
        MSOA_order = self.immigration_to_MSOA.loc[immigration_values.index,'MSOA']

        immigration_values_t = immigration_values.transpose()
        immigration_values_t += 1e-10
        row_sum = immigration_values_t.sum(axis=1)
        immigration_MSOA_rate = immigration_values_t / row_sum[:, None]

        return np.array(immigration_MSOA_rate), MSOA_order.values


def __repr__(self):
        return "ImmigrationDeterministic()"