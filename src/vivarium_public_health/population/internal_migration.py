"""
========================
The Core InternalMigration Model
========================

This module contains tools modeling InternalMigration

"""
import pandas as pd
import numpy as np
from vivarium.framework.utilities import rate_to_probability


class InternalMigration:

    @property
    def name(self):
        return 'integralmigration'

    def setup(self, builder):
        int_outmigration_data = builder.data.load("cause.age_specific_internal_outmigration_rate")

        self.internal_migration_MSOA_location_dict = builder.data.load("internal_migration.MSOA_index")
        self.internal_migration_LAD_location_dict = builder.data.load("internal_migration.LAD_index")

        self.int_out_migration_rate = builder.lookup.build_table(int_outmigration_data, key_columns=['sex', 'location', 'ethnicity'],
                                                                 parameter_columns=['age', 'year'])


        self.int_outmigration_rate = builder.value.register_rate_producer('int_outmigration_rate',
                                                                          source=self.calculate_outmigration_rate,
                                                                          requires_columns=['sex','location','ethnicity'])


        self.random = builder.randomness.get_stream('outmigtation_handler')
        self.clock = builder.time.clock()

        columns_created = ['internal_outmigration','last_outmigration_time','previous_LAD_locations','previous_MSOA_locations']
        view_columns = columns_created + ['alive', 'age', 'sex', 'location','ethnicity','MSOA']
        self.population_view = builder.population.get_view(view_columns)
        builder.population.initializes_simulants(self.on_initialize_simulants,
                                                 creates_columns=columns_created)

        builder.event.register_listener('time_step', self.on_time_step, priority=0)

    def on_initialize_simulants(self, pop_data):
        pop_update = pd.DataFrame({'internal_outmigration': 'No',
                                  'last_outmigration_time': pd.NaT,
                                   'previous_LAD_locations':[''],
                                    'previous_MSOA_locations':['']},
                                  index=pop_data.index)
        self.population_view.update(pop_update)

    def on_time_step(self, event):
        pop = self.population_view.get(event.index, query="alive =='alive' and sex != 'nan'")
        pop['time_since_last_migration'] = event.time - pop['last_outmigration_time']

        # only allow individuals that have not migrated internaly on the last year.
        pop = pop[(pop['time_since_last_migration']>pd.Timedelta("365 days")) | ((pop['time_since_last_migration'].notnull())==False)]

        prob_df = rate_to_probability(pd.DataFrame(self.int_outmigration_rate(pop.index)))
        prob_df['No'] = 1-prob_df.sum(axis=1)
        pop['internal_outmigration'] = self.random.choice(prob_df.index, prob_df.columns, prob_df)
        int_outmigrated_pop = pop.query('internal_outmigration != "No"').copy()

        if not int_outmigrated_pop.empty:
            int_outmigrated_pop['internal_outmigration'] = pd.Series('Yes', index=int_outmigrated_pop.index)
            int_outmigrated_pop['last_outmigration_time'] = event.time

            int_outmigrated_pop['previous_LAD_locations'] += int_outmigrated_pop['location']
            int_outmigrated_pop['previous_MSOA_locations'] += int_outmigrated_pop['MSOA']

            new_MSOA, new_LAD = self.assign_internal_migration(int_outmigrated_pop)

            int_outmigrated_pop['MSOA'] = new_MSOA
            int_outmigrated_pop['location'] = new_LAD

            self.population_view.update(int_outmigrated_pop[['last_outmigration_time', 'internal_outmigration','previous_LAD_locations','previous_MSOA_locations','MSOA','location']])

    def calculate_outmigration_rate(self, index):
        int_out_migration = self.int_out_migration_rate(index)
        return pd.DataFrame({'internal_outmigration': int_out_migration})

    def assign_internal_migration(self,int_migration_pool):

        ''' Based on the characteristic individuals in the internal migration pool, get the relevant
         migration matrix  assign new locations and save the old ones in a new field
         '''

        int_migration_matrix_rate = self.get_migration_matrix(int_migration_pool)


        # sample the rates for each individual and get the new wards.
        c = int_migration_matrix_rate.cumsum(axis=1)
        u = np.random.rand(len(c), 1)
        # get the new MSA
        MSOA_choices = (u < c).argmax(axis=1)

        # from the MSOA index get the new MSOA and LAD location name
        MSOA_choices_name = list(map(self.internal_migration_MSOA_location_dict.get, MSOA_choices))
        LAD_choices_name = list(map(self.internal_migration_LAD_location_dict.get, MSOA_choices))

        return (MSOA_choices_name,LAD_choices_name)

    def get_migration_matrix(self,int_migration_pool):
        '''
        Steps to follow

        1. Process int_migration_pool (MSOA, SEX, AGE fields) to map dictionary keys
        2. With the dictionary keys read relevant columns
        3. Get a matrix of n x m where (n is the number of migrants in the pool, m is the number of potential MSOA they can be assign to).
        4. Normalise the matrix by total number of counts in each row, to obtain rates
        4. Return the rate  matrix of n x m
        '''


        # dummy matrix for now
        int_migration_matrix =  np.random.rand(int_migration_pool.shape[0], len(self.internal_migration_MSOA_location_dict))
        # normalise the matrix to get rates.
        int_migration_matrix_rate = int_migration_matrix / int_migration_matrix.sum(axis=1)[:,None]

        return int_migration_matrix_rate


    def __repr__(self):
        return "InternalMigration()"
