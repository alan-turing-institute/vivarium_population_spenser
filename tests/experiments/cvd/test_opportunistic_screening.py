import os
from collections import defaultdict
from datetime import timedelta, datetime

import numpy as np
import pandas as pd
import pytest

from vivarium import config
from vivarium.framework.event import Event, listens_for
from vivarium.framework.population import uses_columns
from vivarium.test_util import setup_simulation, generate_test_population

from ceam_public_health.risks import ContinuousRiskComponent, distributions, exposures
from ceam_public_health.treatment import HealthcareAccess

from ceam_public_health.experiments.cvd.components.opportunistic_screening import (_hypertensive_categories,
                                                                                   OpportunisticScreening, MEDICATIONS)


def setup():
    try:
        config.reset_layer('override', preserve_keys=['input_data.intermediary_data_cache_path',
                                                      'input_data.auxiliary_data_folder'])
    except KeyError:
        pass
    config.simulation_parameters.set_with_metadata('year_start', 1990, layer='override',
                                                   source=os.path.realpath(__file__))
    config.simulation_parameters.set_with_metadata('year_end', 2010, layer='override',
                                                   source=os.path.realpath(__file__))
    config.simulation_parameters.set_with_metadata('time_step', 30.5, layer='override',
                                                   source=os.path.realpath(__file__))


@listens_for('initialize_simulants', priority=9)
@uses_columns(['high_systolic_blood_pressure_exposure', 'age'])
def _population_setup(event):
    age_sbps = []
    age_sbps.append((40, 130.0))  # Normotensive, below 60
    age_sbps.append((60, 145.0))  # Normotensive, exactly 60
    age_sbps.append((70, 145.0))  # Normotensive, above 60

    age_sbps.append((40, 140.0))  # Hypertensive, below 60
    age_sbps.append((40, 145.0))  # Hypertensive, below 60
    age_sbps.append((60, 170.0))  # Hypertensive, exactly 60
    age_sbps.append((70, 150.0))  # Hypertensive, above 60
    age_sbps.append((70, 155.0))  # Hypertensive, above 60

    age_sbps.append((40, 185.0))  # Severe hypertensive, below 60
    age_sbps.append((70, 185.0))  # Severe hypertensive, above 60

    ages, sbps = zip(*age_sbps)
    population = pd.DataFrame(index=event.index)
    population['age'] = ages
    population['high_systolic_blood_pressure_exposure'] = sbps

    population['age'] = population['age']

    event.population_view.update(population)


@pytest.fixture
def screening_setup():
    module_ = OpportunisticScreening()
    simulation = setup_simulation(
        [generate_test_population, _population_setup, HealthcareAccess(),
         ContinuousRiskComponent('high_systolic_blood_pressure'), module_],
        population_size=10)

    start_time = datetime(1990, 1, 1)
    simulation.current_time = start_time
    return simulation, module_


def test_hypertensive_categories(screening_setup):
    simulation, module_ = screening_setup
    population = simulation.population.population

    normotensive, hypertensive, severe_hypertension = _hypertensive_categories(population)

    assert len(normotensive) == 3
    assert len(hypertensive) == 5
    assert len(severe_hypertension) == 2


def test_drug_effects(screening_setup):
    simulation, module_ = screening_setup
    columns = (['medication_count', 'high_systolic_blood_pressure_exposure']
               + [m['name']+'_supplied_until' for m in MEDICATIONS])
    population_view = simulation.population.get_view(columns)

    starting_sbp = simulation.population.population.high_systolic_blood_pressure_exposure

    event = Event(simulation.population.population.index)
    event.time = datetime(1990, 1, 1)

    # No one is taking any drugs yet so there should be no effect on SBP
    module_.adjust_blood_pressure(event)
    assert (starting_sbp == simulation.population.population.high_systolic_blood_pressure_exposure).all()

    # Now everyone is on the first drug
    population_view.update(pd.Series(1, index=simulation.population.population.index, name='medication_count'))
    for medication in MEDICATIONS:
        population_view.update(pd.Series(simulation.current_time,
                                         index=simulation.population.population.index,
                                         name=medication['name']+'_supplied_until'))
    event.index = simulation.population.population.index
    module_.adjust_blood_pressure(event)
    assert (starting_sbp[simulation.population.population.adherence_category == 'adherent']
            > simulation.population.population.high_systolic_blood_pressure_exposure[
                simulation.population.population.adherence_category == 'adherent']).all()

    efficacy = MEDICATIONS[0]['efficacy']
    adherent_population = simulation.population.population[
        simulation.population.population.adherence_category == 'adherent']
    assert (starting_sbp[adherent_population.index]
            == (adherent_population.high_systolic_blood_pressure_exposure + efficacy)).all()
    non_adherent_population = simulation.population.population[
        simulation.population.population.adherence_category == 'non-adherent']
    assert (starting_sbp[non_adherent_population.index]
            == non_adherent_population.high_systolic_blood_pressure_exposure).all()
    semi_adherent_population = simulation.population.population[
        simulation.population.population.adherence_category == 'semi-adherent']
    assert np.allclose(starting_sbp[semi_adherent_population.index],
                       (semi_adherent_population.high_systolic_blood_pressure_exposure
                        + efficacy*module_.semi_adherent_efficacy))

    # Now everyone is on the first three drugs
    population_view.update(pd.Series(3, index=simulation.population.population.index, name='medication_count'))
    population_view.update(starting_sbp)
    event.index = simulation.population.population.index
    module_.adjust_blood_pressure(event)
    efficacy = sum(m['efficacy'] for m in MEDICATIONS[:3])
    adherent_population = simulation.population.population[
        simulation.population.population.adherence_category == 'adherent']
    assert (starting_sbp[adherent_population.index].round()
            == (adherent_population.high_systolic_blood_pressure_exposure + efficacy).round()).all()
    non_adherent_population = simulation.population.population[
        simulation.population.population.adherence_category == 'non-adherent']
    assert (starting_sbp[non_adherent_population.index]
            == non_adherent_population.high_systolic_blood_pressure_exposure).all()
    semi_adherent_population = simulation.population.population[
        simulation.population.population.adherence_category == 'semi-adherent']
    assert np.allclose(starting_sbp[semi_adherent_population.index],
                       (semi_adherent_population.high_systolic_blood_pressure_exposure
                        + efficacy*module_.semi_adherent_efficacy))


def test_medication_cost(screening_setup):
    simulation, module_ = screening_setup
    module_.cost_by_year = defaultdict(int)

    population_view = simulation.population.get_view(
        ['healthcare_followup_date', 'medication_count']+[m['name']+'_supplied_until' for m in MEDICATIONS])

    # No one is taking drugs yet so there should be no cost
    population_view.update(pd.Series(simulation.current_time + timedelta(days=60),
                                     index=simulation.population.population.index, name='healthcare_followup_date'))
    module_._medication_costs(simulation.population.population, simulation.current_time)
    assert module_.cost_by_year[simulation.current_time.year] == 0
    for medication in MEDICATIONS:
        assert np.all(simulation.population.population[medication['name']+'_supplied_until'].isnull())

    # Now everyone is on one drug
    population_view.update(pd.Series(1, index=simulation.population.population.index, name='medication_count'))
    simulation.last_time_step = timedelta(days=30)
    population_view.update(pd.Series(simulation.current_time + timedelta(days=60),
                                     index=simulation.population.population.index, name='healthcare_followup_date'))

    module_._medication_costs(simulation.population.population, simulation.current_time)

    daily_cost_of_first_medication = MEDICATIONS[0]['daily_cost']
    assert np.allclose(module_.cost_by_year[simulation.current_time.year],
                       daily_cost_of_first_medication * 60 * len(simulation.population.population))
    for medication in MEDICATIONS[1:]:
        assert np.all(simulation.population.population[medication['name'] + '_supplied_until'].isnull())
    assert np.all(simulation.population.population[MEDICATIONS[0]['name'] + '_supplied_until']
                  == simulation.current_time + timedelta(days=60))

    module_.cost_by_year = defaultdict(int)

    # Now everyone is on all the drugs
    population_view.update(pd.Series(len(MEDICATIONS),
                                     index=simulation.population.population.index, name='medication_count'))
    simulation.current_time += timedelta(days=361)  # Force us into the next year
    population_view.update(pd.Series(simulation.current_time + timedelta(days=60),
                                     index=simulation.population.population.index, name='healthcare_followup_date'))
    module_._medication_costs(simulation.population.population, simulation.current_time)
    daily_cost_of_all_medication = sum(m['daily_cost'] for m in MEDICATIONS)
    assert np.allclose(module_.cost_by_year[simulation.current_time.year],
                       daily_cost_of_all_medication * 60 * len(simulation.population.population))
    for medication in MEDICATIONS[1:]:
        assert np.all(simulation.population.population[medication['name'] + '_supplied_until']
                      == simulation.current_time + timedelta(days=60))

    # Now everyone comes back early so they don't need a full sized refill
    simulation.current_time += timedelta(days=45)
    population_view.update(pd.Series(simulation.current_time + timedelta(days=60),
                                     index=simulation.population.population.index, name='healthcare_followup_date'))
    module_.cost_by_year[simulation.current_time.year] = 0
    module_._medication_costs(simulation.population.population, simulation.current_time)

    assert (module_.cost_by_year[simulation.current_time.year]
            == daily_cost_of_all_medication * 45 * len(simulation.population.population))
    for medication in MEDICATIONS[1:]:
        assert np.all(simulation.population.population[medication['name'] + '_supplied_until']
                      == simulation.current_time + timedelta(days=60))

    # This time people come back early again and this time they get a shorter follow up than before.
    simulation.current_time += timedelta(days=1)
    population_view.update(pd.Series(simulation.current_time + timedelta(days=10),
                                     index=simulation.population.population.index, name='healthcare_followup_date'))
    module_.cost_by_year[simulation.current_time.year] = 0
    module_._medication_costs(simulation.population.population, simulation.current_time)

    # Cost should be zero because they have plenty of medication left
    assert module_.cost_by_year[simulation.current_time.year] == 0
    for medication in MEDICATIONS[1:]:
        assert np.all(simulation.population.population[medication['name'] + '_supplied_until']
                      == simulation.current_time + timedelta(days=59))

    # Now they come back for their next appointment and they should have some drugs left over
    simulation.current_time += timedelta(days=10)
    population_view.update(pd.Series(simulation.current_time + timedelta(days=50),
                                     index=simulation.population.population.index, name='healthcare_followup_date'))
    module_.cost_by_year[simulation.current_time.year] = 0
    module_._medication_costs(simulation.population.population, simulation.current_time)

    assert np.allclose(module_.cost_by_year[simulation.current_time.year],
                       daily_cost_of_all_medication * 1 * len(simulation.population.population))
    for medication in MEDICATIONS[1:]:
        assert np.all(simulation.population.population[medication['name'] + '_supplied_until']
                      == simulation.current_time + timedelta(days=50))


# TODO: We need a fixture for the cost table to be able to test this effectively
#def test_blood_pressure_test_cost():
#    simulation, module_ = screening_setup()
#
#    # For the sake of this test, everyone is healthy so we don't have to worry about them getting prescribed drugs
#    # which will change our costs.
#    simulation.population.population['high_systolic_blood_pressure_exposure'] = 112
#
#    # Everybody goes to the hospital
#    simulation.emit_event(PopulationEvent('general_healthcare_access', simulation.population.population))
#    cost_of_a_single_test = config.opportunistic_screening.blood_pressure_test_cost
#    assert (module_.cost_by_year[simulation.current_time.year]
#            == cost_of_a_single_test * len(simulation.population.population))
#
#    # Later, everybody goes to their followup appointment
#    simulation.current_time += timedelta(days=361) # Force us into the next year
#    simulation.emit_event(PopulationEvent('followup_healthcare_access', simulation.population.population))
#    cost_of_a_followup = cost_of_a_single_test + config.appointments.cost
#    assert (module_.cost_by_year[simulation.current_time.year]
#            == cost_of_a_followup * len(simulation.population.population))





# NOTE: If these tests start breaking mysteriously, it's likely because
# something changed the order in which pytest is executing them. They must run
# in the order shown here since they represent a sequence of events with state
# shared through the screening_setup fixture.
def test_general_blood_pressure_test(screening_setup):
    simulation, module_ = screening_setup
    event = Event(simulation.population.population.index)
    simulation.events.get_emitter('general_healthcare_access')(event)
    normotensive, hypertensive, severe_hypertension = _hypertensive_categories(simulation.population.population)
    assert (normotensive.medication_count == 0).all()
    assert (normotensive.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*60)).all()
    assert (hypertensive.medication_count == 0).all()
    assert (hypertensive.healthcare_followup_date == simulation.current_time + timedelta(days=30.5)).all()
    assert (severe_hypertension.medication_count == 2).all()
    assert (severe_hypertension.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*6)).all()

    simulation.current_time += timedelta(days=30) # Tick forward without triggering any actual events
    event = Event(simulation.population.population.index)
    simulation.events.get_emitter('followup_healthcare_access')(event)
    normotensive, hypertensive, severe_hypertension = _hypertensive_categories(simulation.population.population)
    assert (normotensive.medication_count == 0).all()
    assert (normotensive.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*60)).all()
    assert (hypertensive.medication_count == 1).all()
    assert (hypertensive.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*6)).all()
    assert (severe_hypertension.medication_count == 3).all()
    assert (severe_hypertension.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*6)).all()

    simulation.current_time += timedelta(days=30) # Tick forward without triggering any actual events
    event = Event(simulation.population.population.index)
    simulation.events.get_emitter('followup_healthcare_access')(event)
    normotensive, hypertensive, severe_hypertension = _hypertensive_categories(simulation.population.population)
    assert (normotensive.medication_count == 0).all()
    assert (normotensive.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*60)).all()
    assert (hypertensive.medication_count == 2).all()
    assert (hypertensive.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*6)).all()
    assert (severe_hypertension.medication_count == 4).all()
    assert (severe_hypertension.healthcare_followup_date == simulation.current_time + timedelta(days=30.5*6)).all()

    for _ in range(10):
        simulation.current_time += timedelta(days=30) # Tick forward without triggering any actual events
        event = Event(simulation.population.population.index)
        simulation.events.get_emitter('followup_healthcare_access')(event)

    normotensive, hypertensive, severe_hypertension = _hypertensive_categories(simulation.population.population)
    assert (normotensive.medication_count == 0).all()
    assert (hypertensive.medication_count == len(MEDICATIONS)).all()
    assert (severe_hypertension.medication_count == len(MEDICATIONS)).all()
