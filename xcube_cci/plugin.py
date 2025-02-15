# The MIT License (MIT)
# Copyright (c) 2021 by the xcube development team and contributors
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from xcube.constants import EXTENSION_POINT_DATA_OPENERS
from xcube.constants import EXTENSION_POINT_DATA_STORES
from xcube.util import extension

from xcube_cci.constants import CCI_DATA_STORE_ID
from xcube_cci.constants import CDC_DATASET_OPENER_ID
from xcube_cci.constants import CDC_SHORT_DATA_STORE_ID
from xcube_cci.constants import CDC_LONG_DATA_STORE_ID
from xcube_cci.constants import DATASET_OPENER_ID
from xcube_cci.constants import DATA_STORE_ID
from xcube_cci.constants import KC_CCI_DATA_STORE_ID
from xcube_cci.constants import KC_CDC_DATA_STORE_ID
from xcube_cci.constants import KC_DATA_STORE_ID
from xcube_cci.constants import ZARR_CCI_DATA_STORE_ID
from xcube_cci.constants import ZARR_CDC_DATA_STORE_ID
from xcube_cci.constants import ZARR_DATA_STORE_ID


def init_plugin(ext_registry: extension.ExtensionRegistry):
    """xcube CCI ODP extensions"""
    data_completeness_content = \
        'This data store currently provides **only a subset of all ' \
        'datasets** provided by the "ESA CCI Climate Data Centre (CDC), ' \
        'namely gridded datasets originally stored in NetCDF format.\n' \
        'In upcoming versions, the store will also allow for browsing and ' \
        'accessing the remaining CDC datasets. This includes gridded data in ' \
        'TIFF format and also vector data using ESRI Shapefile format.'
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=CCI_DATA_STORE_ID,
        description='ESA Open Data Portal',
        data_store_notices=[dict(id='dataCompleteness',
                                 title='Data Completeness',
                                 content=data_completeness_content,
                                 intent='warning',
                                 icon='warning-sign')]
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=CDC_SHORT_DATA_STORE_ID,
        description='ESA Open Data Portal',
        data_store_notices=[dict(id='dataCompleteness',
                                 title='Data Completeness',
                                 content=data_completeness_content,
                                 intent='warning',
                                 icon='warning-sign')]
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=CDC_LONG_DATA_STORE_ID,
        description='ESA Open Data Portal',
        data_store_notices=[dict(id='dataCompleteness',
                                 title='Data Completeness',
                                 content=data_completeness_content,
                                 intent='warning',
                                 icon='warning-sign')]
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.kcaccess:CciKerchunkDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=KC_CCI_DATA_STORE_ID,
        description='xarray.Dataset in Kerchunk references format'
                    ' from ESA CCI Object Storage'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.kcaccess:CciKerchunkDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=KC_CDC_DATA_STORE_ID,
        description='xarray.Dataset in Kerchunk references format'
                    ' from ESA CCI Object Storage'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.kcaccess:CciKerchunkDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=KC_DATA_STORE_ID,
        description='xarray.Dataset in Kerchunk references format'
                    ' from ESA CCI Object Storage'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.zarraccess:CciZarrDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=ZARR_CCI_DATA_STORE_ID,
        description='xarray.Dataset in Zarr format from ESA CCI Object Storage'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.zarraccess:CciZarrDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=ZARR_CDC_DATA_STORE_ID,
        description='xarray.Dataset in Zarr format from ESA CCI Object Storage'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.zarraccess:CciZarrDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=ZARR_DATA_STORE_ID,
        description='xarray.Dataset in Zarr format'
                    ' from ESA CCI Object Storage'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDataStore'),
        point=EXTENSION_POINT_DATA_STORES,
        name=DATA_STORE_ID,
        description='ESA CCI Open Data Portal',
        data_store_notices=[dict(id='dataCompleteness',
                                 title='Data Completeness',
                                 content=data_completeness_content,
                                 intent='warning',
                                 icon='warning-sign')])
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDatasetOpener'),
        point=EXTENSION_POINT_DATA_OPENERS,
        name=DATASET_OPENER_ID,
        description='xarray.Dataset in Zarr format'
                    ' from ESA CCI Open Data Portal'
    )
    ext_registry.add_extension(
        loader=extension.import_component(
            'xcube_cci.dataaccess:CciOdpDatasetOpener'),
        point=EXTENSION_POINT_DATA_OPENERS,
        name=CDC_DATASET_OPENER_ID,
        description='xarray.Dataset in Zarr format'
                    ' from ESA CCI Open Data Portal'
    )
