from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from vivarium import InteractiveContext
from vivarium_public_health.population.spenser_population import TestPopulation, metadata, build_table

from vivarium_public_health import utilities
from vivarium_public_health.population import Mortality


@pytest.fixture()
def config(base_config):

    # change this to you own path
    path_dir= 'data/'
    # file should have columns -> PID,location,sex,age,ethnicity
    filename = 'Testfile.csv'

    path_to_pop_file= "{}/{}".format(path_dir, filename)
    pop_size = len(pd.read_csv(path_to_pop_file))

    base_config.update({
        'path_to_pop_file':path_to_pop_file,
        'population': {
            'population_size': pop_size,
            'age_start': 0,
            'age_end': 125,
        },
        'time': {
            'step_size': 10,
            }
        }, source=str(Path(__file__).resolve()))
    return base_config


def crude_death_rate_data(live_births=500):
    return (build_table(['mean_value', live_births], 1990, 2017, ('age', 'year', 'sex', 'parameter', 'value'))
            .query('age_start == 25 and sex != "Both"')
            .drop(['age_start', 'age_end'], 'columns'))



def test_Mortality(config, base_plugins):
    pop_size = config.population.population_size
    num_days = 365
    components = [TestPopulation(), Mortality()]
    simulation = InteractiveContext(components=components,
                                    configuration=config,
                                    plugin_configuration=base_plugins,
                                    setup=False)

    asfr_data = build_table(0.5, 2011, 2011).rename(columns={'value': 'mean_value'})

    simulation._data.write("cause.all_causes.cause_specific_mortality_rate", asfr_data)

    simulation.setup()
    simulation.run_for(duration=pd.Timedelta(days=num_days))
    pop = simulation.get_population()

    assert (np.all(pop.alive == 'alive') == False)

