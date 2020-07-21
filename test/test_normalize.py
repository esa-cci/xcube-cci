from unittest import TestCase

import numpy as np
import pandas as pd
import xarray as xr

from xcube_cci.normalize import normalize_cci_dataset

class TestNormalize(TestCase):

    def test_normalize_zonal_lat_lon(self):
        resolution = 10
        lat_size = 3
        lat_coords = np.arange(0, 30, resolution)
        lon_coords = [i + 5. for i in np.arange(-180.0, 180.0, resolution)]

        var_values_1d = xr.DataArray(np.random.random(lat_size), coords=[('latitude_centers', lat_coords)])
        var_values_2d = xr.DataArray(np.array([var_values_1d.values for _ in lon_coords]).T,
                                     coords={'lat': lat_coords, 'lon': lon_coords},
                                     dims=['lat', 'lon'])

        dataset = xr.Dataset({'first': var_values_1d})
        expected = xr.Dataset({'first': var_values_2d})
        expected = expected.assign_coords(
            lon_bnds=xr.DataArray([[i - (resolution / 2), i + (resolution / 2)] for i in expected.lon.values],
                                  dims=['lon', 'bnds']))
        expected = expected.assign_coords(
            lat_bnds=xr.DataArray([[i - (resolution / 2), i + (resolution / 2)] for i in expected.lat.values],
                                  dims=['lat', 'bnds']))
        actual = normalize_cci_dataset(dataset)

        xr.testing.assert_equal(actual, expected)

    def test_normalize_with_missing_time_dim(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        attrs={'time_coverage_start': '20120101',
                               'time_coverage_end': '20121231'})
        norm_ds = normalize_cci_dataset(ds)
        self.assertIsNot(norm_ds, ds)
        self.assertEqual(len(norm_ds.coords), 4)
        self.assertIn('lon', norm_ds.coords)
        self.assertIn('lat', norm_ds.coords)
        self.assertIn('time', norm_ds.coords)
        self.assertIn('time_bnds', norm_ds.coords)

        self.assertEqual(norm_ds.first.shape, (1, 90, 180))
        self.assertEqual(norm_ds.second.shape, (1, 90, 180))
        self.assertEqual(norm_ds.coords['time'][0], xr.DataArray(pd.to_datetime('2012-07-01T12:00:00')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][0], xr.DataArray(pd.to_datetime('2012-01-01')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][1], xr.DataArray(pd.to_datetime('2012-12-31')))

    def test_normalize_with_missing_time_dim_from_filename(self):
        ds = xr.Dataset({'first': (['lat', 'lon'], np.zeros([90, 180])),
                         'second': (['lat', 'lon'], np.zeros([90, 180]))},
                        coords={'lat': np.linspace(-89.5, 89.5, 90),
                                'lon': np.linspace(-179.5, 179.5, 180)},
                        )
        ds_encoding = dict(source='20150204_etfgz_20170309_dtsrgth')
        ds.encoding.update(ds_encoding)
        norm_ds = normalize_cci_dataset(ds)
        self.assertIsNot(norm_ds, ds)
        self.assertEqual(len(norm_ds.coords), 4)
        self.assertIn('lon', norm_ds.coords)
        self.assertIn('lat', norm_ds.coords)
        self.assertIn('time', norm_ds.coords)
        self.assertIn('time_bnds', norm_ds.coords)

        self.assertEqual(norm_ds.first.shape, (1, 90, 180))
        self.assertEqual(norm_ds.second.shape, (1, 90, 180))
        self.assertEqual(norm_ds.coords['time'][0], xr.DataArray(pd.to_datetime('2016-02-21T00:00:00')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][0], xr.DataArray(pd.to_datetime('2015-02-04')))
        self.assertEqual(norm_ds.coords['time_bnds'][0][1], xr.DataArray(pd.to_datetime('2017-03-09')))