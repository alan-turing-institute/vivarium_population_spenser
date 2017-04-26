from math import ceil
from scipy.stats import poisson
from functools import lru_cache

from ceam import config
from ceam.framework.event import listens_for
from ceam.framework.population import creates_simulants
from ceam_inputs.gbd_ms_auxiliary_functions import get_populations

DAYS_PER_YEAR = 365.


@listens_for('time_step', priority=9)
@creates_simulants
def add_new_birth_cohort(event, creator):
    # Assume time step comes to us in days
    time_step_size = config.simulation_parameters.time_step
    annual_new_simulants = config.simulation_parameters.number_of_new_simulants_each_year

    # Assume births are uniformly distributed throughout the year.
    # N.B. Tracking things like leap years, etc., seems silly at this level
    # of model detail, so we don't.
    simulants_to_add = int(ceil(
        annual_new_simulants * time_step_size / DAYS_PER_YEAR))

    creator(simulants_to_add,
            population_configuration={
                'initial_age': 0.0,
            })


@listens_for('time_step', priority=9)
@creates_simulants
def add_new_birth_cohort_nondeterministic(event, creator):
    birth_rate = _get_birth_rate(event.time.year)
    population_size = len(event.index)
    time_step_size = config.simulation_parameters.time_step
    mean_births = birth_rate*population_size*time_step_size/DAYS_PER_YEAR

    # Make the random draws deterministic w/r/t the configuration draw number.
    seed = config.run_configuration.draw_number + event.time.year

    # Assume births occur as a Poisson process
    simulants_to_add = poisson.rvs(mean_births, random_state=seed)

    creator(simulants_to_add,
            population_configuration={
                'initial_age': 0.0,
            })

def _get_birth_rate(year):
    """Computes a crude birth rate from demographic data in a given year.
    
    Parameters
    ----------
    year : int
        The year we want the birth rate for.
    
    Returns
    -------
    float
        The crude birth rate of the population in the given year in 
        births per person per year.
    """
    location_id = config.simulation_parameters.location_id

    # 3 is the sex_id to pull both males and females
    population_table = get_populations(location_id, year, 3)

    population = population_table.pop_scaled.sum()
    births = population_table.pop_scaled[population_table.age<1].sum()

    return births / population





