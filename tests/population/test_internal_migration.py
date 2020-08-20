from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from vivarium import InteractiveContext
from vivarium_public_health.population.spenser_population import TestPopulation, prepare_dataset, transform_rate_table
from vivarium_public_health.population import InternalMigration


@pytest.fixture()
def config(base_config):

    # change this to you own path
    path_dir= 'persistant_data/'

    # setup emigration rates
    # read a dataset from daedalus, change columns to be readable by vivarium
    prepare_dataset(dataset_path="./persistant_data/1000rows_ssm_E08000032_MSOA11_ppp_2011.csv",
                    output_path="./persistant_data/test_ssm_E08000032_MSOA11_ppp_2011.csv"
                    )

    # file should have columns -> PID,location,sex,age,ethnicity
    filename_pop = 'test_ssm_E08000032_MSOA11_ppp_2011.csv'

    # file should have columns -> PID,location,sex,age,ethnicity
    #filename_pop = 'Testfile.csv'

    filename_internal_outmigration_name = 'InternalOutmig2011_LEEDS2.csv'
    path_to_internal_outmigration_file = "{}/{}".format(path_dir, filename_internal_outmigration_name)

    filename_total_population = 'MY2011AGEN.csv'
    path_to_total_population_file = "{}/{}".format(path_dir, filename_total_population)

    path_to_pop_file= "{}/{}".format(path_dir,filename_pop)

    pop_size = len(pd.read_csv(path_to_pop_file))

    base_config.update({

        'path_to_pop_file':path_to_pop_file,
        'path_to_internal_outmigration_file': path_to_internal_outmigration_file,
        'path_to_total_population_file': path_to_total_population_file,

        'population': {
            'population_size': pop_size,
            'age_start': 0,
            'age_end': 100,
        },
        'time': {
            'step_size': 10,
            },
        }, source=str(Path(__file__).resolve()))
    return base_config



def test_internal_outmigration(config, base_plugins):
    start_population_size = config.population.population_size

    num_days = 1
    components = [TestPopulation(), InternalMigration()]
    simulation = InteractiveContext(components=components,
                                    configuration=config,
                                    plugin_configuration=base_plugins,
                                    setup=False)




    df = pd.read_csv(config.path_to_internal_outmigration_file)

    # to save time, only look at locatiosn existing on the test dataset.
    df_internal_outmigration = df#[(df['LAD.code'] == 'E08000032')]

    asfr_data = transform_rate_table(df_internal_outmigration, 2011, 2012, config.population.age_start,
                                     config.population.age_end)

    simulation._data.write("cause.age_specific_internal_outmigration_rate", asfr_data)

    MSOA_location_index = {}
    LAD_location_index = {}

    lad_msoa_df = pd.read_csv('persistant_data/Middle_Layer_Super_Output_Area__2011__to_Ward__2016__Lookup_in_England_and_Wales.csv')
    count = 0
    for i in np.unique(lad_msoa_df['MSOA11CD']):
        MSOA_location_index[count] = i
        LAD_location_index[count] = lad_msoa_df[lad_msoa_df['MSOA11CD']==i]['LAD16CD'].values[0]
        count = count + 1

    simulation._data.write("internal_migration.MSOA_index", MSOA_location_index)
    simulation._data.write("internal_migration.LAD_index", LAD_location_index)


    simulation.setup()


    simulation.run_for(duration=pd.Timedelta(days=num_days))
    pop = simulation.get_population()

    print ('internal outmigration',len(pop[pop['internal_outmigration']=='Yes']))
    print ('remaining population',len(pop[pop['internal_outmigration']=='No']))

    assert (np.all(pop.internal_outmigration == 'Yes') == False)

    assert len(pop[pop['last_outmigration_time']!='NaT']) > 0, 'time of out migration gets saved.'

