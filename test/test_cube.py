import pandas
import unittest

from xcube_cci.config import CubeConfig
from xcube_cci.cube import open_cube

class OpenCubeTest(unittest.TestCase):


    def test_open_cube(self):
        config = CubeConfig(dataset_name='esacci.OZONE.month.L3.NP.multi-sensor.multi-platform.MERGED.fv0002.r1',
                            variable_names=['surface_pressure', 'O3e_du_tot'],
                            geometry=(-5.0,42.0,30.0,58.0),
                            time_range=('1997-01-01', '1997-12-31'),
                            time_period=pandas.Timedelta('30D')
                            )
        cube = open_cube(config)
        self.assertIsNotNone(cube)
        cube.surface_pressure.sel(time='1997-05-01', method='nearest').plot.imshow(vmin=0, vmax=0.2, cmap='Greys_r',
                                                                                   figsize=(16, 10))
