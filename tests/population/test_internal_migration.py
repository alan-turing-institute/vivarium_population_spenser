from pathlib import Path
import numpy as np
import os
import pandas as pd
import pytest
import sys
import wget
from vivarium import InteractiveContext
from vivarium_public_health.population.spenser_population import TestPopulation, prepare_dataset, transform_rate_table
from vivarium_public_health.population import InternalMigration


@pytest.fixture()
def config(base_config):

    # change this to you own path
    path_dir= 'persistant_data/'

    # file should have columns -> PID,location,sex,age,ethnicity
    filename_pop = 'test_ssm_E08000032_MSOA11_ppp_2011.csv'

    # setup emigration rates
    # read a dataset from daedalus, change columns to be readable by vivarium
    prepare_dataset(dataset_path=os.path.join(path_dir, "1000rows_ssm_E08000032_MSOA11_ppp_2011.csv"),
                    output_path=os.path.join(path_dir, filename_pop))

    filename_internal_outmigration_name = 'InternalOutmig2011_LEEDS2.csv'
    path_to_internal_outmigration_file = "{}/{}".format(path_dir, filename_internal_outmigration_name)

    path_to_pop_file= "{}/{}".format(path_dir,filename_pop)
    pop_size = len(pd.read_csv(path_to_pop_file))

    path_msoa_to_lad = os.path.join(path_dir, 'Middle_Layer_Super_Output_Area__2011__to_Ward__2016__Lookup_in_England_and_Wales.csv')
    path_to_OD_matrices = os.path.join(path_dir, "od_matrices")
    path_to_OD_matrix_index_file = os.path.join(path_to_OD_matrices,'MSOA_to_OD_index.csv')



    base_config.update({
        'path_to_pop_file':path_to_pop_file,
        'path_to_internal_outmigration_file': path_to_internal_outmigration_file,
        'path_msoa_to_lad': path_msoa_to_lad,
        'path_to_OD_matrices': path_to_OD_matrices,
        'path_to_OD_matrix_index_file': path_to_OD_matrix_index_file,
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

@pytest.mark.skipif("TRAVIS" in os.environ and os.environ["TRAVIS"] == "true", "Skipping this test on Travis CI.")
def test_internal_outmigration(config, base_plugins):

    num_days = 365*5
    components = [TestPopulation(), InternalMigration()]
    simulation = InteractiveContext(components=components,
                                    configuration=config,
                                    plugin_configuration=base_plugins,
                                    setup=False)

    df = pd.read_csv(config.path_to_internal_outmigration_file)

    # to save time, only look at locations existing on the test dataset.
    df_internal_outmigration = df[df['LAD.code'].isin(['E08000032', 
                                                       'E08000033', 
                                                       'E08000034',
                                                       'E06000024',
                                                       'E08000035',
                                                       'E07000163'])]
    asfr_data = transform_rate_table(df_internal_outmigration, 2011, 2012, config.population.age_start,
                                     config.population.age_end)
    simulation._data.write("cause.age_specific_internal_outmigration_rate", asfr_data)

    # Read MSOA ---> LAD
    msoa_lad_df = pd.read_csv(config.path_msoa_to_lad)
    # Read OD matrix, only destinations
    OD_matrix_dest = pd.read_csv(config.path_to_OD_matrix_index_file,index_col=0)
    OD_matrix_with_LAD = OD_matrix_dest.merge(msoa_lad_df[["MSOA11CD", "LAD16CD"]],left_index=True,
                                                  right_on="MSOA11CD")

    OD_matrix_with_LAD.index = OD_matrix_with_LAD["indices"]

    # Create indices for MSOA and LAD
    MSOA_location_index = OD_matrix_with_LAD["MSOA11CD"].to_dict()
    LAD_location_index = OD_matrix_with_LAD["LAD16CD"].to_dict()

    # Now, read the whole matrix (if it passes the first check)
    simulation._data.write("internal_migration.MSOA_index", MSOA_location_index)
    simulation._data.write("internal_migration.LAD_index", LAD_location_index)
    simulation._data.write("internal_migration.MSOA_LAD_indices", OD_matrix_with_LAD)
    simulation._data.write("internal_migration.path_to_OD_matrices", config.path_to_OD_matrices)

    simulation.setup()


    simulation.run_for(duration=pd.Timedelta(days=num_days))
    pop = simulation.get_population()

    print ('internal outmigration',len(pop[pop['internal_outmigration']=='Yes']))
    print ('remaining population',len(pop[pop['internal_outmigration']=='No']))

    assert (np.all(pop.internal_outmigration == 'Yes') == False)

    assert len(pop[pop['last_outmigration_time']!='NaT']) > 0, 'time of out migration gets saved.'
    assert len(pop[pop['previous_MSOA_locations']!='']) > 0, 'previous location of the migrant gets saved.'