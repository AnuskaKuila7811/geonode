#########################################################################
#
# Copyright (C) 2018 OSGeo
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#########################################################################
"""Utilities for enabling OGC STA remote services in geonode."""
import json
import logging
import requests
import traceback
import geojson

from uuid import uuid4
from urllib.parse import (
    urljoin,
    unquote,
    quote,
    urlparse,
    urlsplit,
    urlunparse,
    urlencode,
    parse_qsl,
    ParseResult,
)

from django.conf import settings
from django.urls import reverse
from django.db.models import Q
from django.template.defaultfilters import slugify
from django.utils.translation import ugettext as _

from geonode.base import enumerations as base_enumerations
from geonode.base.models import (
    Link,
    ResourceBase,
    TopicCategory)
from geonode.layers.models import Dataset
from geonode.base.bbox_utils import BBOXHelper
from geonode.layers.utils import resolve_regions
from geonode.utils import http_client, get_legend_url
from geonode.resource.manager import resource_manager
from geonode.thumbs.thumbnails import create_thumbnail

from owslib import __version__
from owslib.util import clean_ows_url, http_get, Authentication
from datetime import datetime, timedelta

from .. import enumerations
from ..enumerations import CASCADED
from ..enumerations import INDEXED
from .. import models
from .. import utils
from . import base
from collections import namedtuple

logger = logging.getLogger(__name__)

REQUEST_HEADERS = {
    'User-Agent': 'OWSLib {} (https://geopython.github.io/OWSLib)'.format(__version__)
}
DATASTREAMS = 'Datastreams'
FEATURES_OF_INTEREST = 'FeaturesOfInterest'
OBSERVED_PROPERTIES = 'ObservedProperties'
SENSORS = 'Sensors'
THINGS = 'Things'
FILTER_OBSERVATION_DATASTREAM_ID_EQ = "?$filter=Observations/Datastream/id eq '"
FILTER_DATASTREAMS_ID_EQ = "?$filter=Datastreams/id eq '"
SELECT_ID_NAME = "$select=id,name"
VALUE = 'value'
NAME = 'name'
DESCRIPTION = 'description'
IOT_ID = '@iot.id'
IOT_NEXT_LINK = '@iot.nextLink'

Datastream = namedtuple("Datastream",
                        "id, \
                      title, \
                      abstract")


def get_proxified_sta_url(url, version='1.1', proxy_base=None):
    """
    clean an STA URL of basic 
    """

    if url is None or not url.startswith('http'):
        return url

    parsed = urlparse(url)

    base_sta_url = urlunparse([
        parsed.scheme,
        parsed.netloc,
        parsed.path,
        parsed.params,
        '',
        parsed.fragment
    ])

    sta_url = quote(base_sta_url, safe='')
    proxified_url = f"{proxy_base}?url={sta_url}"
    return (version, proxified_url, base_sta_url)


def SensorThingsService(url,
                        version='1.1',
                        username=None,
                        password=None,
                        parse_remote_metadata=False,
                        timeout=30,
                        headers=None,
                        proxy_base=None):
    """
    API for SensorThings-API (STA) methods and metadata.
    """
    '''sta factory function, returns a version specific SensorThingsService object

    @type url: string
    @param url: url of STA root document
    @type parse_remote_metadata: boolean
    @param parse_remote_metadata: whether to fully process MetadataURL elements
    @param timeout: time (in seconds) after which requests should timeout
    @return: initialized SensorThingsService_1_1_0 object
    '''

    if not proxy_base:
        clean_url = url
        base_sta_url = clean_url
    else:
        (clean_version, proxified_url, base_sta_url) = get_proxified_sta_url(
            url, version=version, proxy_base=proxy_base)
        version = clean_version
        clean_url = proxified_url

    if version in ['1.1']:
        return (
            base_sta_url,
            SensorThingsService_1_1(
                clean_url, version=version,
                parse_remote_metadata=parse_remote_metadata,
                username=username, password=password,
                timeout=timeout, headers=headers
            )
        )
        """
    elif version in ['1.0']:
        return (
            base_sta_url,
            SensorThingsService_1_0(
                clean_url, version=version,
                parse_remote_metadata=parse_remote_metadata,
                username=username, password=password,
                timeout=timeout, headers=headers
            )
        )
        """
    raise NotImplementedError(
        f'The STA version ({version}) you requested is not implemented. Please use 1.0 or 1.1.')


class StaServiceHandler(base.ServiceHandlerBase):
    """Remote service handler for OGC STA services"""

    service_type = enumerations.STA

    def __init__(self, url):
        base.ServiceHandlerBase.__init__(self, url)
        self.proxy_base = urljoin(
            settings.SITEURL, reverse('proxy'))
        self.url = url
        self._parsed_service = None
        self.indexing_method = INDEXED
        self.name = slugify(self.url)[:255]

    @property
    def parsed_service(self):
        cleaned_url = self.url
        ogc_server_settings = settings.OGC_SERVER['default']
        _url, _parsed_service = SensorThingsService(
            cleaned_url,
            version='1.1',
            proxy_base=None,
            timeout=ogc_server_settings.get('TIMEOUT', 60))
        return _parsed_service

    def create_cascaded_store(self):
        return None

    def create_geonode_service(self, owner, parent=None):
        """Create a new geonode.service.models.Service instance
        :arg owner: The user who will own the service instance
        :type owner: geonode.people.models.Profile

        """
        instance = models.Service(
            uuid=str(uuid4()),
            base_url=self.url,
            proxy_base=None,  # self.proxy_base,
            type=self.service_type,
            method=self.indexing_method,
            owner=owner,
            parent=parent,
            metadata_only=True,
            version=1.1,
            name=self.name,
            title=self.name,
            abstract=_(
                "Not provided"),
            online_resource=self.parsed_service.url,

        )
        """
        version=self.parsed_service.identification.version,
        name=self.name,
        title=self.parsed_service.identification.title or self.name,
        abstract=self.parsed_service.identification.abstract or _(
            "Not provided"),
        online_resource=self.parsed_service.provider.url,
        """
        instance.save()
        return instance

    def get_keywords(self):
        keywords = set()
        keywords.update(self.parsed_service.getObservedPropertyNames())
        keywords.update(self.parsed_service.getFeatureOfInterestNames())
        keywords.update(self.parsed_service.getSensorNames())
        keywords.update(self.parsed_service.getThingNames())
        return keywords

    def _get_keywords_for_resource(self, resource_id):
        keywords = set()
        keywords.update(self.parsed_service.getObservedPropertyNamesForDatastream(resource_id))
        keywords.update(self.parsed_service.getFeatureOfInterestNamesForDatastream(resource_id))
        keywords.update(self.parsed_service.getSensorNamesForDatastream(resource_id))
        keywords.update(self.parsed_service.getThingNamesForDatastream(resource_id))
        return keywords

    def get_resource(self, resource_id):
        return self.parsed_service.getDatastream(identifier=resource_id)
        """
        ll = None
        logger.error(f"Cache {ll}")
        try:
            ll = self.parsed_service.getDatastream(identifier=resource_id)
            logger.error(f"Cache {ll}")
        except Exception as e:
            logger.exception(e)
        return self._dataset_meta(ll) if ll else None
        """

    def get_resources(self):
        """Return an iterable with the service's resources.

        For WMS we take into account that some layers are just logical groups
        of metadata and do not return those.

        contents_gen = self.parsed_service.getDatastreams()
        return (r for r in contents_gen if not any(r.children))
        """
        try:
            return self._parse_datasets(self.parsed_service.getDatastreams())
        except Exception:
            traceback.print_exc()
            return None

    def _parse_datasets(self, datastreams):
        map_datasets = []
        for ds in datastreams:
            map_datasets.append(self._dataset_meta(ds))
        return map_datasets

    def _dataset_meta(self, datastream):
        _ll = {}
        _ll['id'] = datastream[IOT_ID] if IOT_ID in datastream else None
        _ll['title'] = datastream[NAME] if NAME in datastream else None
        _ll['abstract'] = datastream[DESCRIPTION] if DESCRIPTION in datastream else None
        return Datastream(**_ll)

    def harvest_resource(self, resource_id, geonode_service):
        """Harvest a single resource from the service

        This method will try to create new ``geonode.layers.models.Dataset``
        instance (and its related objects too).

        :arg resource_id: The resource's identifier
        :type resource_id: str
        :arg geonode_service: The already saved service instance
        :type geonode_service: geonode.services.models.Service

        """
        dataset_meta = self.get_resource(resource_id)
        resource_fields = self._get_indexed_dataset_fields(dataset_meta)
        keywords = resource_fields.pop("keywords")
        existance_test_qs = Dataset.objects.filter(
            name=resource_fields["name"],
            store=resource_fields["store"],
            workspace=resource_fields["workspace"]
        )
        if existance_test_qs.exists():
            raise RuntimeError(
                f"Resource {resource_id} has already been harvested")
        resource_fields["keywords"] = keywords
        resource_fields["is_approved"] = True
        resource_fields["is_published"] = True
        if settings.RESOURCE_PUBLISHING or settings.ADMIN_MODERATE_UPLOADS:
            resource_fields["is_approved"] = False
            resource_fields["is_published"] = False
        geonode_dataset = self._create_dataset(geonode_service, **resource_fields)
        self._create_dataset_service_link(geonode_dataset)
        # self._create_dataset_legend_link(geonode_dataset)
        # self._create_dataset_thumbnail(geonode_dataset)

    def has_resources(self):
        return self.parsed_service.hasDatastreams()

    def _create_dataset(self, geonode_service, **resource_fields):
        # bear in mind that in ``geonode.layers.models`` there is a
        # ``pre_save_dataset`` function handler that is connected to the
        # ``pre_save`` signal for the Dataset model. This handler does a check
        # for common fields (such as abstract and title) and adds
        # sensible default values
        keywords = resource_fields.pop("keywords", [])
        defaults = dict(
            owner=geonode_service.owner,
            remote_service=geonode_service,
            remote_typename=geonode_service.name,
            sourcetype=base_enumerations.SOURCE_TYPE_REMOTE,
            ptype=getattr(geonode_service, "ptype", "gxp_wmscsource"),
            **resource_fields
        )
        if geonode_service.method == INDEXED:
            defaults['ows_url'] = geonode_service.service_url

        geonode_dataset = resource_manager.create(
            None,
            resource_type=Dataset,
            defaults=defaults
        )
        resource_manager.update(geonode_dataset.uuid, instance=geonode_dataset, keywords=keywords, notify=True)
        resource_manager.set_permissions(geonode_dataset.uuid, instance=geonode_dataset)

        return geonode_dataset

    def _create_dataset_service_link(self, geonode_dataset):
        ogc_sta_url = geonode_dataset.ows_url
        ogc_sta_name = f'OGC STA: {geonode_dataset.store} Service'
        ogc_sta_link_type = 'OGC:STA'
        if Link.objects.filter(resource=geonode_dataset.resourcebase_ptr,
                               name=ogc_sta_name,
                               link_type=ogc_sta_link_type,).count() < 2:
            Link.objects.update_or_create(
                resource=geonode_dataset.resourcebase_ptr,
                name=ogc_sta_name,
                link_type=ogc_sta_link_type,
                defaults=dict(
                    extension='html',
                    url=ogc_sta_url,
                    mime='text/html',
                    link_type=ogc_sta_link_type
                )
            )
    
    def __get_bbox(self, dataset_meta):
        bbox = dataset_meta['observedArea']
        if bbox is not None:
            bbox = utils.decimal_encode(dataset_meta['observedArea'])
        if bbox is None:
            features = self.parsed_service.getFeaturesForDatastream(dataset_meta[IOT_ID])
            x_list = list()
            y_list = list()
            for f in features:
                feature = geojson.loads(json.dumps(f['feature']))
                x_list.append(feature.coordinates[0])
                y_list.append(feature.coordinates[1])
            bbox = min(x_list), max(x_list), min(y_list), max(y_list)
        if len(bbox) < 4:
            raise RuntimeError(
                f"Resource BBOX is not valid: {bbox}")
        return bbox

    def _get_indexed_dataset_fields(self, dataset_meta):
        bbox = self.__get_bbox(dataset_meta)
        typename = slugify(f"{dataset_meta[IOT_ID]}-{''.join(c for c in dataset_meta[NAME] if ord(c) < 128)}")
        return {
            "name": dataset_meta[NAME],
            "store": self.name,
            "subtype": "remote",
            "workspace": "remoteWorkspace",
            "typename": typename,
            "alternate": dataset_meta[NAME],
            "title": dataset_meta[NAME],
            "abstract": dataset_meta[DESCRIPTION],
            "bbox_polygon": BBOXHelper.from_xy([bbox[0], bbox[2], bbox[1], bbox[3]]).as_polygon(),
            "srid": bbox[4] if len(bbox) > 4 else "EPSG:4326",
            "keywords": [keyword[:100] for keyword in self._get_keywords_for_resource(resource_id=dataset_meta[IOT_ID])],
        }


class BaseSensorThingsService:
    """ Abstraction of OGC SensorThingsAPI """

    def __init__(self, url, version='1.1', xml=None, username=None, password=None,
                 parse_remote_metadata=False, headers=None, timeout=30, auth=None):
        """ """
        if '?' in url:
            self.url, self.url_query_string = url.split('?')
        else:
            self.url = url.rstrip('/') + '/'
            self.url_query_string = None

        self.timeout = timeout
        self.headers = REQUEST_HEADERS
        if headers:
            self.headers.update(headers)
        self.auth = auth
        self.datastreams = None

    def __get_json_data(self, url):
        return http_get(url=url, headers=self.headers, auth=self.auth).json()

    def __get_value(self, key, response):
        if key in response:
            return response[key]
        else:
            return None

    def __get_next_link(self, response):
        return self.__get_value(IOT_NEXT_LINK, response)

    def __get_name(self, data):
        l = list()
        for item in data:
            l.append(item[NAME] if item[NAME] is not None else item[IOT_ID])
        return l

    def __get_elements(self, url):
        response = self.__get_json_data(url)
        if response is not None:
            elements = self.__get_value(VALUE, response)
            nextLink = self.__get_next_link(response)
            if elements is not None:
                while nextLink is not None:
                    response = self.__get_json_data(nextLink).json()
                    if response is not None:
                        nextLink = self.__get_next_link(response)
                        temp = self.__get_value(VALUE, response)
                        if response is not None:
                            elements.extend(temp)
                return elements

    def __get_by_identifier(self, resource, identifier):
        data = self.__get_json_data(self.url.__add__(resource).__add__('(\'').__add__(identifier).__add__('\')'))
        if data['error'] is not None:
            data = self.__get_json_data(self.url.__add__(resource).__add__('(').__add__(identifier).__add__(')'))
        return data

    def __update_datastreams(self):
        self.datastreams = self.__get_elements(self.url.__add__(DATASTREAMS))
        self.lastUpdate = datetime.now()

    def getDatastreams(self):
        if self.datastreams is None:
            self.__update_datastreams()
        elif datetime.now() - timedelta(hours=1) > self.lastUpdate:
            self.__update_datastreams()
        return self.datastreams

    def getDatastream(self, identifier):
        return self.__get_by_identifier(DATASTREAMS, identifier)

    def hasDatastreams(self):
        data = self.__get_json_data(self.url.__add__(DATASTREAMS).__add__('?$count=true&$top=1'))
        count = self.__get_value('@iot.count', data)
        return True if count is not None and count > 0 else False

    def getFeaturesOfInterest(self):
        return self.__get_elements(self.url.__add__(FEATURES_OF_INTEREST))

    def getFeaturesForDatastream(self, id):
        query = self.url.__add__(FEATURES_OF_INTEREST).__add__(FILTER_OBSERVATION_DATASTREAM_ID_EQ).__add__(id).__add__("'")
        return self.__get_elements(query)

    def getFeatureOfInterestNamesForDatastream(self, id):
        query = self.url.__add__(FEATURES_OF_INTEREST).__add__(FILTER_OBSERVATION_DATASTREAM_ID_EQ).__add__(id).__add__("'").__add__("&").__add__(SELECT_ID_NAME)
        data = self.__get_elements(query)
        return self.__get_name(data)

    def getFeatureOfInterestNames(self):
        data = self.__get_elements(self.url.__add__(FEATURES_OF_INTEREST).__add__("?").__add__(SELECT_ID_NAME))
        return self.__get_name(data)

    def getObservedPropertyNames(self):
        data = self.__get_elements(self.url.__add__(OBSERVED_PROPERTIES).__add__("?").__add__(SELECT_ID_NAME))
        return self.__get_name(data)

    def getObservedPropertyNamesForDatastream(self, id):
        query = self.url.__add__(OBSERVED_PROPERTIES).__add__(FILTER_DATASTREAMS_ID_EQ).__add__(id).__add__("'").__add__("&").__add__(SELECT_ID_NAME)
        data = self.__get_elements(query)
        return self.__get_name(data)

    def getSensorNames(self):
        data = self.__get_elements(self.url.__add__(SENSORS).__add__("?").__add__(SELECT_ID_NAME))
        return self.__get_name(data)

    def getSensorNamesForDatastream(self, id):
        query = self.url.__add__(SENSORS).__add__(FILTER_DATASTREAMS_ID_EQ).__add__(id).__add__("'").__add__("&").__add__(SELECT_ID_NAME)
        data = self.__get_elements(query)
        return self.__get_name(data)

    def getThingNames(self):
        data = self.__get_elements(self.url.__add__(THINGS).__add__("?").__add__(SELECT_ID_NAME))
        return self.__get_name(data)

    def getThingNamesForDatastream(self, id):
        query = self.url.__add__(THINGS).__add__(FILTER_DATASTREAMS_ID_EQ).__add__(id).__add__("'").__add__("&").__add__(SELECT_ID_NAME)
        data = self.__get_elements(query)
        return self.__get_name(data)


class SensorThingsService_1_1(BaseSensorThingsService):

    def get_version(self):
        return '1.1'
