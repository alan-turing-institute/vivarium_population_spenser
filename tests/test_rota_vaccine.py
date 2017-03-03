import pandas as pd
import numpy as np
from ceam_public_health.components.diarrhea_disease_model import diarrhea_factory
from ceam_public_health.components.interventions.rota_vaccine import determine_who_should_receive_dose
from ceam_tests.util import pump_simulation, generate_test_population, setup_simulation
from ceam import config
from ceam.framework.event import Event
from ceam_inputs import generate_ceam_population
from ceam_public_health.components.interventions.rota_vaccine import RotaVaccine
import pytest
from ceam_public_health.components.base_population import generate_base_population

def test_determine_who_should_receive_dose():
    """ 
    Determine if people are receiving the correct dosages. Move the simulation forward a few times to make sure that people who should get the vaccine do get the vaccine
    """
    factory = diarrhea_factory()

    simulation = setup_simulation([generate_test_population, RotaVaccine()] + factory)

    pop = simulation.population.population

    pop['rotaviral_entiritis_vaccine_first_dose'] = 0

    pop['age'] = config.getint('rota_vaccine', 'age_at_first_dose') / 365

    first_dose_pop = determine_who_should_receive_dose(pop, pop.index, 'rotaviral_entiritis_vaccine_first_dose', 1)

    # FIXME: This test will fail in years in which there is vaccination coverage in the baseline scenario
    assert np.allclose(len(pop)*config.getfloat('rota_vaccine', 'vaccination_proportion_increase'),  len(first_dose_pop), .1), "determine who should receive dose needs to give doses at the correct age"

    first_dose_pop['rotaviral_entiritis_vaccine_second_dose'] = 0

    first_dose_pop['age'] = (config.getint('rota_vaccine', 'age_at_first_dose') + 61) / 365

    second_dose_pop = determine_who_should_receive_dose(first_dose_pop, first_dose_pop.index, 'rotaviral_entiritis_vaccine_second_dose', 2)

    # FIXME: This test will fail in years in which there is vaccination coverage in the baseline scenario
    assert np.allclose(len(pop)*config.getfloat('rota_vaccine', 'vaccination_proportion_increase')*config.getfloat('rota_vaccine', 'second_dose_retention'),  len(second_dose_pop), .1), "determine who should receive dose needs to give doses at the correct age"

    second_dose_pop['rotaviral_entiritis_vaccine_third_dose'] = 0

    second_dose_pop['age'] = (config.getint('rota_vaccine', 'age_at_first_dose') + 61 + 61) / 365

    third_dose_pop = determine_who_should_receive_dose(second_dose_pop, second_dose_pop.index, 'rotaviral_entiritis_vaccine_third_dose', 3)

    # FIXME: This test will fail in years in which there is vaccination coverage in the baseline scenario
    assert np.allclose(len(pop)*config.getfloat('rota_vaccine', 'vaccination_proportion_increase')*config.getfloat('rota_vaccine', 'second_dose_retention')*config.getfloat('rota_vaccine', 'third_dose_retention'),  len(third_dose_pop), .1), "determine who should receive dose needs to give doses at the correct age"

def test_incidence_rate():
    """
    Set vaccine working column for only some people, ensure that their diarrhea due to rota incidence is reduced by the vaccine_effectiveness specified in the config file
    """
    factory = diarrhea_factory()

    # FIXME: This test only works if population all start at the same age
    simulation = setup_simulation([generate_base_population] + factory)

    pop = simulation.population.population

    pop['rotaviral_entiritis_vaccine_first_dose'] = 0

    pop['age'] = config.getint('rota_vaccine', 'age_at_first_dose') / 365

    first_dose_pop = determine_who_should_receive_dose(pop, pop.index, 'rotaviral_entiritis_vaccine_first_dose', 1)

    first_dose_pop['rotaviral_entiritis_vaccine_second_dose'] = 0

    first_dose_pop['age'] = (config.getint('rota_vaccine', 'age_at_first_dose') + 61) / 365

    second_dose_pop = determine_who_should_receive_dose(first_dose_pop, first_dose_pop.index, 'rotaviral_entiritis_vaccine_second_dose', 2)

    second_dose_pop['rotaviral_entiritis_vaccine_third_dose'] = 0

    second_dose_pop['age'] = (config.getint('rota_vaccine', 'age_at_first_dose') + 61 + 61) / 365

    third_dose_pop = determine_who_should_receive_dose(second_dose_pop, second_dose_pop.index, 'rotaviral_entiritis_vaccine_third_dose', 3)

    # Identify the simulants that did not receive the vaccine
    key_diff = set(pop.index).difference(third_dose_pop.index)
    where_diff = pop.index.isin(key_diff)

    not_vaccinated = pop[where_diff]

    rota_inc = simulation.values.get_rate('incidence_rate.diarrhea_due_to_rotaviral_entiritis')

    RotaVaccine.incidence_rates(simulation.population.population.index, rota_inc, simulation.population.population)

    # find an example of simulants of the same age and sex, but not vaccination status, and then compare their incidence rates
    assert pd.unique(rota_inc(not_vaccinated[0:1].index)) * config.getfloat('rota_vaccine', 'total_vaccine_effectiveness') == pd.unique(rota_inc(third_dose_pop[1:2].index)), "the vaccine should reduce the incidence of diarrhea due to rotavirus according to the effectiveness specified in the config file"

#End.
