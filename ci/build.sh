#!/usr/bin/env sh

set -euo pipefail

[[ -d $PWD/maven && ! -d $HOME/.m2 ]] && ln -s $PWD/maven $HOME/.m2

r2dbc_over_adba_artifactory=$(pwd)/r2dbc-over-adba-artifactory
r2dbc_spi_artifactory=$(pwd)/r2dbc-spi-artifactory

rm -rf $HOME/.m2/repository/io/r2dbc 2> /dev/null || :

cd r2dbc-over-adba
./mvnw deploy \
    -DaltDeploymentRepository=distribution::default::file://${r2dbc_over_adba_artifactory} \
    -Dr2dbcSpiArtifactory=file://${r2dbc_spi_artifactory}
