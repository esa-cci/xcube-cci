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

from xcube.core.store import DataStoreError
from xcube.util.jsonschema import JsonObjectSchema
from xcube_cci.zarraccess import CciZarrDataStore


@unittest.skipIf(os.environ.get('XCUBE_DISABLE_WEB_TESTS', None) == '1',
                 'XCUBE_DISABLE_WEB_TESTS = 1')
class CciZarrDataStoreTest(unittest.TestCase):

    def setUp(self) -> None:
        self.store = CciZarrDataStore()

    def test_get_data_store_params_schema(self):
        data_store_params_schema = self.store.get_data_store_params_schema()
        self.assertIsNotNone(data_store_params_schema)
        self.assertEqual(JsonObjectSchema(additional_properties=False).to_dict(),
                         data_store_params_schema.to_dict())

    def test_get_data_types(self):
        self.assertEqual({'mldataset', 'geodataframe', 'dataset'},
                         set(self.store.get_data_types()))

    @unittest.skipIf(os.environ.get('XCUBE_CCI_DISABLE_WEB_TESTS', '1') == '1',
            'XCUBE_CCI_DISABLE_WEB_TESTS = 1')
    def test_get_data_ids(self):
        data_ids = self.store.list_data_ids()
        self.assertIsNotNone(data_ids)

    @unittest.skipIf(os.environ.get('XCUBE_CCI_DISABLE_WEB_TESTS', '1') == '1',
            'XCUBE_CCI_DISABLE_WEB_TESTS = 1')
    def test_open_data(self):
        ds = self.store.open_data("ESACCI-L3C_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-1982-2016-fv3.0.zarr")
        self.assertIsNotNone(ds)

    def test_get_open_data_params_schema(self):
        schema = self.store.get_open_data_params_schema()
        self.assertIn('log_access', schema.properties.keys())
        self.assertIn('cache_size', schema.properties.keys())
        self.assertIn('chunks', schema.properties.keys())
        self.assertIn('consolidated', schema.properties.keys())
        self.assertIn('data_type', schema.properties.keys())
        self.assertIn('decode_cf', schema.properties.keys())
        self.assertIn('decode_coords', schema.properties.keys())
        self.assertIn('decode_times', schema.properties.keys())
        self.assertIn('drop_variables', schema.properties.keys())
        self.assertIn('group', schema.properties.keys())
        self.assertIn('mask_and_scale', schema.properties.keys())
        self.assertEqual([], schema.required)

    def test_get_data_writer_ids(self):
        self.assertEqual((), self.store.get_data_writer_ids())

    def test_get_write_data_params_schema(self):
        self.assertEqual(JsonObjectSchema(additional_properties=False).to_dict(),
                         self.store.get_write_data_params_schema().to_dict())

    def test_write_data(self):
        import xarray as xr
        data = xr.Dataset()
        with self.assertRaises(DataStoreError) as dse:
            self.store.write_data(data)
        self.assertEqual('The CciZarrDataStore is read-only.',
                         f'{dse.exception}')

    def test_delete_data(self):
        with self.assertRaises(DataStoreError) as dse:
            self.store.delete_data('some_data_id')
        self.assertEqual('The CciZarrDataStore is read-only.',
                         f'{dse.exception}')
