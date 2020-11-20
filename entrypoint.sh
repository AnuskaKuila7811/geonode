#!/bin/bash
set -e

/usr/local/bin/invoke update 2>&1

source $HOME/.bashrc
source $HOME/.override_env

echo DATABASE_URL=$DATABASE_URL
echo GEODATABASE_URL=$GEODATABASE_URL
echo SITEURL=$SITEURL
echo ALLOWED_HOSTS=$ALLOWED_HOSTS
echo GEOSERVER_PUBLIC_LOCATION=$GEOSERVER_PUBLIC_LOCATION
echo MONITORING_ENABLED=$MONITORING_ENABLED
echo MONITORING_HOST_NAME=$MONITORING_HOST_NAME
echo MONITORING_SERVICE_NAME=$MONITORING_SERVICE_NAME
echo MONITORING_DATA_TTL=$MONITORING_DATA_TTL

/usr/local/bin/invoke waitfordbs 2>&1
echo "ENTRYPOINT: waitfordbs task done"

echo "ENTRYPOINT: running migrations"
/usr/local/bin/invoke migrations 2>&1
echo "ENTRYPOINT: migrations task done"

cmd="$@"

echo DOCKER_ENV=$DOCKER_ENV

if [ -z ${DOCKER_ENV} ] || [ ${DOCKER_ENV} = "development" ]
then
    echo "ENTRYPOINT: Executing standard Django server $cmd for Development"
else
    if [ ${IS_CELERY} = "true" ]  || [ ${IS_CELERY} = "True" ]
    then
        cmd=$CELERY_CMD
        echo "ENTRYPOINT: Executing Celery server $cmd for Production"
    else

        /usr/local/bin/invoke prepare 2>&1
        echo "ENTRYPOINT: prepare task done"

        if [ ${IS_FIRST_START} = "true" ] || [ ${IS_FIRST_START} = "True" ] || [ ${FORCE_REINIT} = "true" ]  || [ ${FORCE_REINIT} = "True" ] || [ ! -e "/mnt/volumes/statics/geonode_init.lock" ]; then
            /usr/local/bin/invoke updategeoip 2>&1
            echo "ENTRYPOINT: updategeoip task done"
            /usr/local/bin/invoke fixtures 2>&1
            echo "ENTRYPOINT: fixture task done"
            /usr/local/bin/invoke monitoringfixture 2>&1
            echo "ENTRYPOINT: monitoringfixture task done"
            /usr/local/bin/invoke initialized 2>&1
            echo "ENTRYPOINT: initialized"
        fi

        echo "ENTRYPOINT: refresh static data"
        /usr/local/bin/invoke statics 2>&1
        echo "ENTRYPOINT: static data refreshed"
        /usr/local/bin/invoke waitforgeoserver 2>&1
        echo "ENTRYPOINT: waitforgeoserver task done"
        /usr/local/bin/invoke geoserverfixture 2>&1
        echo "ENTRYPOINT: geoserverfixture task done"
        /usr/local/bin/invoke updateadmin 2>&1
        echo "ENTRYPOINT: updateadmin task done"

        cmd=$UWSGI_CMD
        echo "ENTRYPOINT: Executing UWSGI server $cmd for Production"
    fi
fi
echo "ENTRYPOINT: got command $cmd"
exec $cmd
