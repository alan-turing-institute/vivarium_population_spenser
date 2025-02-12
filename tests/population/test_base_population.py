from pathlib import Path
import math

import numpy as np
import pandas as pd
import pytest
from vivarium import InteractiveContext
from vivarium.testing_utilities import get_randomness

from vivarium_population_spenser import utilities
import vivarium_population_spenser.population.base_population as bp
import vivarium_population_spenser.population.data_transformations as dt
from vivarium_population_spenser.testing.utils import make_uniform_pop_data


@pytest.fixture
def config(base_config):
    base_config.update({
        'population': {
            'age_start': 0,
            'age_end': 110,
        },
    }, source=str(Path(__file__).resolve()), layer='model_override')
    return base_config


@pytest.fixture
def generate_population_mock(mocker):
    return mocker.patch('vivarium_population_spenser.population.base_population.generate_population')


@pytest.fixture
def age_bounds_mock(mocker):
    return mocker.patch('vivarium_population_spenser.population.base_population._assign_demography_with_age_bounds')


@pytest.fixture
def initial_age_mock(mocker):
    return mocker.patch('vivarium_population_spenser.population.base_population._assign_demography_with_initial_age')


def make_base_simulants():
    simulant_ids = range(100000)
    creation_time = pd.Timestamp(1990, 7, 2)
    return pd.DataFrame({'entrance_time': pd.Series(pd.Timestamp(creation_time), index=simulant_ids),
                         'exit_time': pd.Series(pd.NaT, index=simulant_ids),
                         'alive': pd.Series('alive', index=simulant_ids)},
                        index=simulant_ids)


def make_full_simulants():
    base_simulants = make_base_simulants()
    base_simulants['location'] = pd.Series(1, index=base_simulants.index)
    base_simulants['sex'] = pd.Series('Male', index=base_simulants.index).astype(
        pd.api.types.CategoricalDtype(categories=['Male', 'Female'], ordered=False))
    base_simulants['age'] = np.random.uniform(0, 100, len(base_simulants))
    base_simulants['tracked'] = pd.Series(True, index=base_simulants.index)
    return base_simulants


def test_select_sub_population_data():
    data = pd.DataFrame({'year_start': [1990, 1995, 2000, 2005],
                         'year_end': [1995, 2000, 2005, 2010],
                         'population': [100, 110, 120, 130]})

    sub_pop = bp.BasePopulation.select_sub_population_data(data, 1997)

    assert sub_pop.year_start.values.item() == 1995


def test_BasePopulation(config, base_plugins, generate_population_mock):
    num_days = 600
    time_step = 100  # Days
    sims = make_full_simulants()
    start_population_size = len(sims)

    generate_population_mock.return_value = sims.drop(columns=['tracked'])

    base_pop = bp.BasePopulation()

    components = [base_pop]
    config.update({'population': {'population_size': start_population_size},
                   'time': {'step_size': time_step}}, layer='override')
    simulation = InteractiveContext(components=components,
                                    configuration=config,
                                    plugin_configuration=base_plugins)
    time_start = simulation._clock.time

    pop_structure = simulation._data.load('population.structure')
    pop_structure['location'] = simulation.configuration.input_data.location
    uniform_pop = dt.assign_demographic_proportions(pop_structure)

    assert base_pop.population_data.equals(uniform_pop)

    age_params = {'age_start': config.population.age_start,
                  'age_end': config.population.age_end}
    sub_pop = bp.BasePopulation.select_sub_population_data(uniform_pop, time_start.year)

    generate_population_mock.assert_called_once()
    # Get a dictionary of the arguments used in the call
    mock_args = generate_population_mock.call_args[1]
    assert mock_args['creation_time'] == time_start - simulation._clock.step_size
    assert mock_args['age_params'] == age_params
    assert mock_args['population_data'].equals(sub_pop)
    assert mock_args['randomness_streams'] == base_pop.randomness
    pop = simulation.get_population()
    for column in pop:
        assert pop[column].equals(sims[column])

    final_ages = pop.age + num_days / utilities.DAYS_PER_YEAR

    simulation.run_for(duration=pd.Timedelta(days=num_days))

    pop = simulation.get_population()
    assert np.allclose(pop.age, final_ages, atol=0.5 / utilities.DAYS_PER_YEAR)  # Within a half of a day.


def test_age_out_simulants(config, base_plugins):
    start_population_size = 10000
    num_days = 600
    time_step = 100  # Days
    config.update({'population': {
        'population_size': start_population_size,
        'age_start': 4,
        'age_end': 4,
        'exit_age': 5,
    },
        'time': {'step_size': time_step}
    }, layer='override')
    components = [bp.BasePopulation()]
    simulation = InteractiveContext(components=components,
                                    configuration=config,
                                    plugin_configuration=base_plugins)
    time_start = simulation._clock.time

    assert len(simulation.get_population()) == len(simulation.get_population().age.unique())
    simulation.run_for(duration=pd.Timedelta(days=num_days))
    pop = simulation.get_population()
    assert len(pop) == len(pop[~pop.tracked])
    exit_after_300_days = pop.exit_time >= time_start + pd.Timedelta(300, unit='D')
    exit_before_400_days = pop.exit_time <= time_start + pd.Timedelta(400, unit='D')
    assert len(pop) == len(pop[exit_after_300_days & exit_before_400_days])


def test_generate_population_age_bounds(age_bounds_mock, initial_age_mock):
    creation_time = pd.Timestamp(1990, 7, 2)
    step_size = pd.Timedelta(days=1)
    age_params = {'age_start': 0,
                  'age_end': 120}
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    r = {k: get_randomness() for k in ['general_purpose', 'bin_selection', 'age_smoothing']}
    sims = make_base_simulants()
    simulant_ids = sims.index

    bp.generate_population(simulant_ids, creation_time, step_size,
                           age_params, pop_data, r, lambda *args, **kwargs: None)

    age_bounds_mock.assert_called_once()
    mock_args = age_bounds_mock.call_args[0]
    assert mock_args[0].equals(sims)
    assert mock_args[1].equals(pop_data)
    assert mock_args[2] == float(age_params['age_start'])
    assert mock_args[3] == float(age_params['age_end'])
    assert mock_args[4] == r
    initial_age_mock.assert_not_called()


def test_generate_population_initial_age(age_bounds_mock, initial_age_mock):
    creation_time = pd.Timestamp(1990, 7, 2)
    step_size = pd.Timedelta(days=1)
    age_params = {'age_start': 0,
                  'age_end': 0}
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    r = {k: get_randomness() for k in ['general_purpose', 'bin_selection', 'age_smoothing']}
    sims = make_base_simulants()
    simulant_ids = sims.index

    bp.generate_population(simulant_ids, creation_time, step_size,
                           age_params, pop_data, r, lambda *args, **kwargs: None)

    initial_age_mock.assert_called_once()
    mock_args = initial_age_mock.call_args[0]
    assert mock_args[0].equals(sims)
    assert mock_args[1].equals(pop_data)

    assert mock_args[2] == float(age_params['age_start'])
    assert mock_args[3] == step_size
    assert mock_args[4] == r
    age_bounds_mock.assert_not_called()


def test__assign_demography_with_initial_age(config):
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    pop_data = pop_data[pop_data.year_start == 1990]
    simulants = make_base_simulants()
    initial_age = 20
    r = {k: get_randomness() for k in ['general_purpose', 'bin_selection', 'age_smoothing']}
    step_size = pd.Timedelta(days=config.time.step_size)

    simulants = bp._assign_demography_with_initial_age(simulants, pop_data, initial_age,
                                                       step_size, r, lambda *args, **kwargs: None)

    assert len(simulants) == len(simulants.age.unique())
    assert simulants.age.min() > initial_age
    assert simulants.age.max() < initial_age + utilities.to_years(step_size)
    assert math.isclose(len(simulants[simulants.sex == 'Male']) / len(simulants), 0.5, abs_tol=0.01)
    for location in simulants.location.unique():
        assert math.isclose(len(simulants[simulants.location == location]) / len(simulants),
                            1 / len(simulants.location.unique()), abs_tol=0.01)


def test__assign_demography_with_initial_age_zero(config):
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    pop_data = pop_data[pop_data.year_start == 1990]
    simulants = make_base_simulants()
    initial_age = 0
    r = {k: get_randomness() for k in ['general_purpose', 'bin_selection', 'age_smoothing']}
    step_size = utilities.to_time_delta(config.time.step_size)

    simulants = bp._assign_demography_with_initial_age(simulants, pop_data, initial_age,
                                                       step_size, r, lambda *args, **kwargs: None)

    assert len(simulants) == len(simulants.age.unique())
    assert simulants.age.min() > initial_age
    assert simulants.age.max() < initial_age + utilities.to_years(step_size)
    assert math.isclose(len(simulants[simulants.sex == 'Male']) / len(simulants), 0.5, abs_tol=0.01)
    for location in simulants.location.unique():
        assert math.isclose(len(simulants[simulants.location == location]) / len(simulants),
                            1 / len(simulants.location.unique()), abs_tol=0.01)


def test__assign_demography_with_initial_age_error():
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    pop_data = pop_data[pop_data.year_start == 1990]
    simulants = make_base_simulants()
    initial_age = 200
    r = {k: get_randomness() for k in ['general_purpose', 'bin_selection', 'age_smoothing']}
    step_size = pd.Timedelta(days=1)

    with pytest.raises(ValueError):
        bp._assign_demography_with_initial_age(simulants, pop_data, initial_age,
                                               step_size, r, lambda *args, **kwargs: None)


def test__assign_demography_with_age_bounds():
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    pop_data = pop_data[pop_data.year_start == 1990]
    simulants = make_base_simulants()
    age_start, age_end = 0, 180
    r = {k: get_randomness(k) for k in ['general_purpose', 'bin_selection', 'age_smoothing', 'age_smoothing_age_bounds']}

    simulants = bp._assign_demography_with_age_bounds(simulants, pop_data, age_start,
                                                      age_end, r, lambda *args, **kwargs: None)

    assert math.isclose(len(simulants[simulants.sex == 'Male']) / len(simulants), 0.5, abs_tol=0.01)

    for location in simulants.location.unique():
        assert math.isclose(len(simulants[simulants.location == location]) / len(simulants),
                            1 / len(simulants.location.unique()), abs_tol=0.01)
    ages = np.sort(simulants.age.values)
    age_deltas = ages[1:] - ages[:-1]

    age_bin_width = 5  # See `make_uniform_pop_data`
    num_bins = len(pop_data.age.unique())
    n = len(simulants)
    assert math.isclose(age_deltas.mean(), age_bin_width * num_bins / n, rel_tol=1e-3)
    assert age_deltas.max() < 100 * age_bin_width * num_bins / n  # Make sure there are no big age gaps.


def test__assign_demography_with_age_bounds_error():
    pop_data = dt.assign_demographic_proportions(make_uniform_pop_data(age_bin_midpoint=True))
    simulants = make_base_simulants()
    age_start, age_end = 110, 120
    r = {k: get_randomness() for k in ['general_purpose', 'bin_selection', 'age_smoothing']}

    with pytest.raises(ValueError):
        bp._assign_demography_with_age_bounds(simulants, pop_data, age_start,
                                              age_end, r, lambda *args, **kwargs: None)
