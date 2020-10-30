# Vivarium Population Spenser

Welcome to the Vivarium Population Spenser repository.


This library is used by [``Daedalous``](https://github.com/alan-turing-institute/daedalus) spatial microsimulation pipeline that allows users to produce 
(custom) population projections for the whole of the United Kingdom at the local authority (LA) level.

This library is being developed in collaboration between Leeds Institute for Institute Data Analytics and the Alan Turing 
Institute as part of the SPENSER (Synthetic Population Estimation and Scenario Projection Model) project.


This library is largely based in the [``vivarium_public_health``](https://github.com/ihmeuw/vivarium_public_health.git) library,
but it has been modified to cater for the needs of the SPENSER project.

## Installation

To install the Vivarium Population Spenser library follow the next steps:

  ``> git clone https://github.com/alan-turing-institute/vivarium_population_spenser.git``

  ``> cd vivarium_population_spenser``

  ``> python install .``
  
 
 If you plan to run the microsimulation using this library we recommend it to run if from the [``Daedalous``](https://github.com/alan-turing-institute/daedalus) pipeline 
 that contains many useful scripts to interact with this library.

## Documentation


The Vivarium Population Spenser library contains 6 main components used to simulate the
evolution of an input population. You can find all the computational workflow protocols of all of these components in [here](https://www.protocols.io/workspaces/spenser).


There components are the following:

## Population

The [Spenser Population component](src/vivarium_population_spenser/spenser_population.py) is used define the starting point of
 the simulation from an input population (```input_population```) to be used in the microsimulation. 
 It creates a dataframe with following fields:

```
  population = pd.DataFrame(
            {'age': input_population['age'],
             'entrance_time': creation_time,
             'sex': input_population['sex'],
             'alive': pd.Series('alive', index=index),
             'location': input_population['location'],
             'ethnicity': input_population['ethnicity'],
             'exit_time': pd.NaT,
             'MSOA': input_population['MSOA']},
            index=index)
```

where ```creation_time``` is the starting time of the simulation.
 
## Mortality:

The [mortality](src/vivarium_population_spenser/mortality.py) module contains tools modeling all cause mortality based on individuals characteristics as
gender, age, location (local authority level) and ethnicity.

This module uses as input table to establish the morality rates the table [Mortality2011_LEEDS1_2.csv](persistant_data/Mortality2011_LEEDS1_2.csv).

Details about the computational workflow protocol followed by this component can be found here: [dx.doi.org/10.17504/protocols.io.bn79mhr6](dx.doi.org/10.17504/protocols.io.bn79mhr6) 

### Fertility:

A model of [fertility](src/vivarium_population_spenser/add_new_birth_cohorts.py) based on individual characteristics as, age, location (local authority level) and ethnicity.

This module uses as input table to establish the morality rates the table [Fertility2011_LEEDS1_2.csv](persistant_data/Fertility2011_LEEDS1_2.csv).

Details about the computational workflow protocol followed by this component can be found here: [dx.doi.org/10.17504/protocols.io.bn8bmhsn](dx.doi.org/10.17504/protocols.io.bn8bmhsn)

## Emigration:

This module models [emigration](src/vivarium_population_spenser/emigration.py) based on individuals characteristics as
gender, age, location (local authority level) and ethnicity.

This module uses as input table to establish the morality rates the table [Emig_2011_2012_LEEDS2.csv](persistant_data/Emig_2011_2012_LEEDS2.csv).

Details about the computational workflow protocol followed by this component can be found here: [dx.doi.org/10.17504/protocols.io.bn8emhte](dx.doi.org/10.17504/protocols.io.bn8emhte). 

## Immigration

A deterministic [immigration](src/vivarium_population_spenser/immigration.py) model in which the total number of immigrants is read from a file
level and the characteristics of the immigrants are sampled from the migration rate file.

The immigrants are assigned to a local authority and then based on their age and gender to an MSOA. 

The input table to establish the total number of immigrants
 and their characteristics [Emig_2011_2012_LEEDS2.csv](persistant_data/Emig_2011_2012_LEEDS2.csv).

The input table to assigned them an MSOA [Immigration_MSOA_M_F.csv](persistant_data/Immigration_MSOA_M_F.csv).

Details about the computational workflow protocol followed by this component can be found here: [dx.doi.org/10.17504/protocols.io.bn9dmh26](dx.doi.org/10.17504/protocols.io.bn9dmh26)


## Internal migration

This module models  the [internal_migration](src/vivarium_population_spenser/internal_migration.py) between MSOAs (and their
respective LADs) of individuals based on their gender, age, initial location (local authority level) and ethnicity.

The input table to establish the pool of migrants that internally migrate based on 
 their characteristics is [InternalOutmig2011_LEEDS2.csv](persistant_data/InternalOutmig2011_LEEDS2.csv).
 
 Once the pool of migrants is chosen they are assigned to a new MSOA based on their age and gender using the
 MSOA migration matrices in [od_matrices](persistant_data/od_matrices).
 
 Details about the computational workflow protocol followed by this component can be found here: [dx.doi.org/10.17504/protocols.io.bn9imh4e](dx.doi.org/10.17504/protocols.io.bn9imh4e) 
 
 
# Note:

For details of how all the tables were produced, please contact Nik Lomax and Luke Archer. 
