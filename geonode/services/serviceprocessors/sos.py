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
"""Utilities for enabling OGC SOS remote services in geonode."""
import logging
import traceback

from uuid import uuid4
from urllib.parse import (
    urljoin,
    unquote,
    urlparse,
    urlencode,
    parse_qsl,
    ParseResult,
)

from django.conf import settings
from django.urls import reverse
from django.db.models import Q
from django.template.defaultfilters import slugify
from django.utils.translation import ugettext as _
from owslib.swe.observation import sos100
import sos4py

from geonode.base import enumerations as base_enumerations
from geonode.base.models import (
    Link,
    ResourceBase)
from geonode.layers.models import Dataset
from geonode.base.bbox_utils import BBOXHelper
from geonode.layers.utils import resolve_regions
from geonode.utils import http_client, get_legend_url
from geonode.resource.manager import resource_manager
from geonode.thumbs.thumbnails import create_thumbnail

from sos4py import sos_2_0_0, util
from owslib.sos import sos200
from owslib.util import clean_ows_url

from .. import enumerations
from ..enumerations import CASCADED
from ..enumerations import INDEXED
from .. import models
from .. import utils
from . import base
from collections import namedtuple

logger = logging.getLogger(__name__)

Offering = namedtuple("Offering",
                        "id, \
                      title, \
                      abstract")

def SensorObservationService(url,
                  version='2.0.0',
                  xml=None,
                  username=None,
                  password=None,
                  parse_remote_metadata=False,
                  timeout=30,
                  headers=None,
                  proxy_base=None):
    """
    API for Sensor Observation Service (SOS) methods and metadata.
    """
    '''sos factory function, returns a version specific WebMapService object

    @type url: string
    @param url: url of SOS capabilities document
    @type xml: string
    @param xml: elementtree object
    @type parse_remote_metadata: boolean
    @param parse_remote_metadata: whether to fully process MetadataURL elements
    @param timeout: time (in seconds) after which requests should timeout
    @return: initialized SensorObservationService_2_0_0 object
    '''

    if not proxy_base:
        clean_url = clean_ows_url(url)
        base_ows_url = clean_url
    else:
        (clean_version, proxified_url, base_ows_url) = base.get_proxified_ows_url(
            url, version=version, proxy_base=proxy_base)
        version = clean_version
        clean_url = proxified_url

    if version in ['1.0.0']:
        return (
            base_ows_url,
            sos100.SensorObservationService_1_0_0(
                clean_url, version=version, xml=xml,
                username=username, password=password
            )
        )
    elif version in ['2.0.0', '2.0']:
        return (            base_ows_url,
            sos_2_0_0.sos_2_0_0.__new__(sos_2_0_0.sos_2_0_0,
                clean_url, version=version, xml=xml,
                username=username, password=password
            )
        )
    raise NotImplementedError(
        f'The SOS version ({version}) you requested is not implemented. Please use 1.0.0 or 2.0.0.')


class SosServiceHandler(base.ServiceHandlerBase,
                        base.CascadableServiceHandlerMixin):
    """Remote service handler for OGC SOS services"""

    service_type = enumerations.SOS

    def __init__(self, url):
        base.ServiceHandlerBase.__init__(self, url)
        self.proxy_base = urljoin(
            settings.SITEURL, reverse('proxy'))
        self.url = url
        self._parsed_service = None
        self.indexing_method = INDEXED
        self.name = slugify(self.url)[:255]

    @staticmethod
    def get_cleaned_url_params(url):
        # Unquoting URL first so we don't loose existing args
        url = unquote(url)
        # Extracting url info
        parsed_url = urlparse(url)
        # Extracting URL arguments from parsed URL
        get_args = parsed_url.query
        # Converting URL arguments to dict
        parsed_get_args = dict(parse_qsl(get_args))
        # Strip out redoundant args
        _version = parsed_get_args.pop('version', '2.0.0') if 'version' in parsed_get_args else '2.0.0'
        _service = parsed_get_args.pop('service') if 'service' in parsed_get_args else None
        _request = parsed_get_args.pop('request') if 'request' in parsed_get_args else None
        # Converting URL argument to proper query string
        encoded_get_args = urlencode(parsed_get_args, doseq=True)
        # Creating new parsed result object based on provided with new
        # URL arguments. Same thing happens inside of urlparse.
        new_url = ParseResult(
            parsed_url.scheme, parsed_url.netloc, parsed_url.path,
            parsed_url.params, encoded_get_args, parsed_url.fragment
        )
        return (new_url, _service, _version, _request)

    @property
    def parsed_service(self):
        cleaned_url, service, version, request = SosServiceHandler.get_cleaned_url_params(self.url)
        _url, _parsed_service = SensorObservationService(
            cleaned_url.geturl(),
            version=version,
            proxy_base=None)
        return _parsed_service

    def create_cascaded_store(self, service):
        ogc_sos_url = service.service_url
        ogc_sos_get_capabilities = service.operations.get('GetCapabilities', None)
        if ogc_sos_get_capabilities and ogc_sos_get_capabilities.get('methods', None):
            for _op_method in ogc_sos_get_capabilities.get('methods'):
                if _op_method.get('type', None).upper() == 'GET' and _op_method.get('url', None):
                    ogc_sos_url = _op_method.get('url')

        store = self._get_store(create=True)
        store.capabilitiesURL = ogc_sos_url
        cat = store.catalog
        cat.save(store)
        return store

    def create_geonode_service(self, owner, parent=None):
        """Create a new geonode.service.models.Service instance
        :arg owner: The user who will own the service instance
        :type owner: geonode.people.models.Profile

        """
        cleaned_url, service, version, request = SosServiceHandler.get_cleaned_url_params(self.url)
        operations = {}
        for _op in self.parsed_service.operations:
            try:
                _methods = []
                for _op_method in (getattr(_op, 'methods', []) if hasattr(_op, 'methods') else _op.get('methods', [])):
                    _methods.append(
                        {
                            'type': _op_method.get('type', None),
                            'url': _op_method.get('url', None)
                        }
                    )

                _name = getattr(_op, 'name', None) if hasattr(_op, 'name') else _op.get('name', None)
                _formatOptions = getattr(_op, 'formatOptions', []) if hasattr(_op, 'formatOptions') else _op.get('formatOptions', [])
                if _name:
                    operations[_name] = {
                        'name': _name,
                        'methods': _methods,
                        'formatOptions': _formatOptions
                    }
            except Exception as e:
                logger.exception(e)
        instance = models.Service(
            uuid=str(uuid4()),
            base_url=f"{cleaned_url.scheme}://{cleaned_url.netloc}{cleaned_url.path}".encode("utf-8", "ignore").decode('utf-8'),
            extra_queryparams=cleaned_url.query,
            proxy_base=None,  # self.proxy_base,
            type=self.service_type,
            method=self.indexing_method,
            owner=owner,
            parent=parent,
            metadata_only=True,
            version=str(self.parsed_service.identification.version).encode("utf-8", "ignore").decode('utf-8'),
            name=self.name,
            title=str(self.parsed_service.identification.title).encode("utf-8", "ignore").decode('utf-8') or self.name,
            abstract=str(self.parsed_service.identification.abstract).encode("utf-8", "ignore").decode('utf-8') or _(
                "Not provided"),
            operations=operations,
            online_resource=self.parsed_service.provider.url,
        )
        return instance

    def get_keywords(self):
        if self.parsed_service.identification.keywords is not None and len(self.parsed_service.identification.keywords) > 0:
            return self.parsed_service.identification.keywords
        else:
            keywords = set()
            for offering in self.parsed_service.contents.values():
                keywords.update(offering.observed_properties)
                keywords.update(offering.procedures)
                keywords.update(offering.features_of_interest)
            return keywords

    def _get_keywords_for_resource(self, offering):
        keywords = set()
        keywords.update(offering.observed_properties)
        keywords.update(offering.procedures)
        keywords.update(offering.features_of_interest)
        return keywords

    def get_resource(self, resource_id):
        return self.parsed_service.contents[resource_id]

    def get_resources(self):
        """Return an iterable with the service's resources.

        For SOS we take into account that some layers are just logical groups
        of metadata and do not return those.

        """
        try:
            return self._parse_offerings(self.parsed_service.contents.values())
        except Exception:
            traceback.print_exc()
            return None

    def _parse_offerings(self, offerings):
        map_offerings = []
        for ds in offerings:
            map_offerings.append(self._offering_meta(ds))
        return map_offerings

    def _offering_meta(self, offering):
        _ll = {}
        _ll['id'] = offering.id
        _ll['title'] = offering.name
        _ll['abstract'] = offering.description
        return Offering(**_ll)

    def harvest_resource(self, resource_id, geonode_service):
        """Harvest a single resource from the service

        This method will try to create new ``geonode.layers.models.Dataset``
        instance (and its related objects too).

        :arg resource_id: The resource's identifier
        :type resource_id: str
        :arg geonode_service: The already saved service instance
        :type geonode_service: geonode.services.models.Service

        """
        offering = self.get_resource(resource_id)
        logger.debug(f"dataset_meta: {offering}")
        resource_fields = self._get_indexed_dataset_fields(offering)
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
        self._create_dataset_service_link(geonode_service, geonode_dataset)

    def has_resources(self):
        return True if len(self.parsed_service.contents) > 0 else False

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
            ptype=getattr(geonode_service, "ptype", "gxp_wmscsource") or "gxp_wmscsource",
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

    def _create_dataset_thumbnail(self, geonode_service, geonode_dataset):
        """Create a thumbnail with a SOS request."""
        create_thumbnail(
            instance=geonode_dataset,
            sos_version=self.parsed_service.version,
            bbox=geonode_dataset.bbox,
            forced_crs=geonode_dataset.srid if 'EPSG:' in str(geonode_dataset.srid) else f'EPSG:{geonode_dataset.srid}',
            overwrite=True,
        )

    def _create_dataset_service_link(self, geonode_service, geonode_dataset):
        ogc_sos_link_type = 'OGC:SOS'
        ogc_sos_name = f'OGC SOS: {geonode_dataset.store} Service'

        ogc_sos_url = geonode_dataset.ows_url
        ogc_sos_get_capabilities = geonode_service.operations.get('GetCapabilities', None)
        if ogc_sos_get_capabilities and ogc_sos_get_capabilities.get('methods', None):
            for _op_method in ogc_sos_get_capabilities.get('methods'):
                if _op_method.get('type', None).upper() == 'GET' and _op_method.get('url', None):
                    ogc_sos_url = _op_method.get('url')
                    geonode_dataset.ows_url = ogc_sos_url
                    Dataset.objects.filter(id=geonode_dataset.id).update(ows_url=ogc_sos_url)
                    break

        if Link.objects.filter(resource=geonode_dataset.resourcebase_ptr,
                               name=ogc_sos_name,
                               link_type=ogc_sos_link_type,).count() < 2:
            Link.objects.update_or_create(
                resource=geonode_dataset.resourcebase_ptr,
                name=ogc_sos_name,
                link_type=ogc_sos_link_type,
                defaults=dict(
                    extension='html',
                    url=ogc_sos_url,
                    mime='text/html',
                    link_type=ogc_sos_link_type
                )
            )

    def _get_indexed_dataset_fields(self, offering):
        bbox = offering.bbox
        if len(bbox) < 4:
            raise RuntimeError(
                f"Resource BBOX is not valid: {bbox}")
        typename = slugify(f"{offering.id}-{''.join(c for c in offering.name if ord(c) < 128)}")
        return {
            "name": offering.id,
            "store": self.name,
            "subtype": "remote",
            "workspace": "remoteWorkspace",
            "typename": typename,
            "alternate": offering.id,
            "title": offering.name,
            "abstract": offering.description,
            "bbox_polygon": BBOXHelper.from_xy([bbox[0], bbox[2], bbox[1], bbox[3]]).as_polygon(),
            "srid": offering.bbox_srs if offering.bbox_srs is not None else "EPSG:4326",
            "keywords": [keyword[:100] for keyword in self._get_keywords_for_resource(offering)],
            "temporal_extent_start": offering.begin_position,
            "temporal_extent_end": offering.end_position,
        }

    def _get_store(self, create=True):
        """Return the geoserver store associated with this service.

        The store may optionally be created if it doesn't exist already.
        Store is assumed to be (and created) named after the instance's name
        and belongs to the default geonode workspace for cascaded layers.

        """
        workspace = base.get_geoserver_cascading_workspace(create=create)
        cat = workspace.catalog
        store = cat.get_store(self.name, workspace=workspace)
        logger.debug(f"name: {self.name}")
        logger.debug(f"store: {store}")
        if store is None and create:  # store did not exist. Create it
            store = cat.create_sosstore(
                name=self.name,
                workspace=workspace,
                user=cat.username,
                password=cat.password
            )
        return store
