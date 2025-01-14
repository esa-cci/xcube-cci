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

import os
import unittest

import xarray as xr

from xcube.core.store import DataStore
from xcube.core.store import MutableDataStore
from xcube.core.store import DatasetDescriptor
from xcube.core.store import DataType
from xcube_cci.kcaccess import CciKerchunkDataStore

# os.environ['XCUBE_CCI_DISABLE_WEB_TESTS'] = '0'


@unittest.skipIf(os.environ.get('XCUBE_CCI_DISABLE_WEB_TESTS', '1') == '1',
                 'XCUBE_CCI_DISABLE_WEB_TESTS = 1')
class CciKerchunkDataStoreOpenTest(unittest.TestCase):
    store = CciKerchunkDataStore()

    def test_open_data(self):
        data_id = "ESACCI-SOILMOISTURE-L3S-SSMS-ACTIVE-19910805-20211231-fv07.1_kr1.0"
        dataset = self.store.open_data(data_id)
        self.assertIsInstance(dataset, xr.Dataset)
        self.assertEqual({'time': 11107, 'lat': 720, 'lon': 1440}, dataset.sizes)


class CciKerchunkDataStoreTest(unittest.TestCase):
    store = CciKerchunkDataStore()

    def test_has_store(self):
        self.assertIsInstance(self.store, DataStore)
        self.assertNotIsInstance(self.store, MutableDataStore)

    def test_get_data_store_params_schema(self):
        self.assertIsNotNone(self.store.get_data_store_params_schema())

    def test_list_data_ids(self):
        data_ids = self.store.list_data_ids()
        self.assertIn(
            "ESACCI-BIOMASS-L4-AGB-CHANGE-100m-2018-2017-fv4.0-kr1.1",
            data_ids
        )
        self.assertIn(
            "ESACCI-L3C_CLOUD-CLD_PRODUCTS-MERIS-AATSR_ENVISAT-200301-201112_fv2.0_kr1.0",
            data_ids
        )
        self.assertIn(
            "CCI_GMB_GIS_DTU_fv1.5_kr1.0",
            data_ids
        )

    def test_get_data_opener_ids(self):
        self.assertEqual(("dataset:zarr:reference",), self.store.get_data_opener_ids())

    def test_get_data_types(self):
        self.assertEqual(("dataset",), self.store.get_data_types())
        self.assertEqual(("dataset",),
                         self.store.get_data_types_for_data("sst-cube"))

    def test_has_data(self):
        store = self.store
        data_id = 'ESACCI-L4_FIRE-BA-MODIS-20010101-20200120-fv5.1-kr1.2'
        self.assertEqual(True, store.has_data(data_id))
        self.assertEqual(False, store.has_data("lst-cube"))

    def test_describe_data(self):
        data_id = 'ESACCI-L4_FIRE-BA-MODIS-20010101-20200120-fv5.1-kr1.2'
        descriptor = self.store.describe_data(data_id)
        self.assertIsInstance(descriptor, DatasetDescriptor)
        self.assertEqual(data_id, descriptor.data_id)
        self.assertIsInstance(descriptor.data_type, DataType)
        self.assertIs(xr.Dataset, descriptor.data_type.dtype)
        self.assertIsInstance(descriptor.bbox, tuple)
        self.assertIsNone(descriptor.spatial_res)  # ?
        self.assertIsInstance(descriptor.dims, dict)
        self.assertIsInstance(descriptor.coords, dict)
        self.assertIsInstance(descriptor.data_vars, dict)
        self.assertIsInstance(descriptor.attrs, dict)

    def test_get_search_params_schema(self):
        # We do not have search parameters yet
        self.assertEqual(
            {
                "type": "object",
                "properties": {}
            },
            self.store.get_search_params_schema().to_dict()
        )

    def test_search_data(self):
        search_results = list(self.store.search_data())
        self.assertEqual(164, len(search_results))
        for descriptor, data_id in zip(search_results, self.store.get_data_ids()):
            self.assertIsInstance(descriptor, DatasetDescriptor)
            self.assertEqual(data_id, descriptor.data_id)
            self.assertIsInstance(descriptor.data_type, DataType)
            self.assertIs(xr.Dataset, descriptor.data_type.dtype)
            self.assertIsInstance(descriptor.dims, dict)
            self.assertIsInstance(descriptor.data_vars, dict)
            self.assertIsInstance(descriptor.attrs, dict)
