import unittest

import jsonschema
from xcube.core.store import DatasetDescriptor
from xcube.util.jsonschema import JsonObjectSchema

from xcube_cci.dtaccess import DataTreeDescriptor


class DataTreeDescriptorTest(unittest.TestCase):
    def test_get_schema(self):
        schema = DataTreeDescriptor.get_schema()
        self.assertIsInstance(schema, JsonObjectSchema)

    def test_from_dict_no_data_id(self):
        descriptor_dict = dict()
        with self.assertRaises(jsonschema.exceptions.ValidationError):
            DataTreeDescriptor.from_dict(descriptor_dict)

    def test_from_dict_wrong_data_type(self):
        descriptor_dict = dict(data_id="xyz", data_type="tsr")
        with self.assertRaises(ValueError) as cm:
            DataTreeDescriptor.from_dict(descriptor_dict)
        self.assertEqual("unknown data type 'tsr'", f"{cm.exception}")

    def test_from_dict_derived_type(self):
        descriptor_dict = dict(data_id="xyz", data_type="datatree")
        descriptor = DataTreeDescriptor.from_dict(descriptor_dict)
        self.assertIsNotNone(descriptor)
        self.assertEqual("xyz", descriptor.data_id)
        self.assertEqual("datatree", descriptor.data_type.alias)

    def test_from_dict_full(self):
        descriptor_dict = dict(
            data_id="xyz",
            data_type="datatree",
            crs="EPSG:9346",
            bbox=(10.0, 20.0, 30.0, 40.0),
            spatial_res=20.0,
            time_range=("2017-06-05", "2017-06-27"),
            time_period="daily",
            coords=dict(
                rtdt=dict(
                    name="rtdt",
                    dtype="rj",
                    dims=("rtdt",),
                    attrs=dict(ssd=6, zjgrhgu="hgtr"),
                )
            ),
            dims=dict(x=1, y=2, z=3),
            data_vars=dict(
                xf=dict(
                    name="xf",
                    dtype="rj",
                    dims=("dfjhrt", "sg"),
                    attrs=dict(ssd=4, zjgrhgu="dgfrf"),
                )
            ),
            dataset=dict(
                data_id="abc",
                data_type="dataset",
            ),
            data_nodes=dict(
                first_inner_tree=dict(
                    data_id="first_inner_tree",
                    data_type="datatree",
                ),
                second_inner_tree=dict(
                    data_id="second_inner_tree",
                    data_type="datatree",
                )
            ),
            attrs=dict(dzus=236, tgr7h="rt5", df="s8fd4w5"),
            open_params_schema=dict(
                type="object",
                properties=dict(
                    variable_names=dict(type="array", items=dict(type="string"))
                ),
            ),
        )
        descriptor = DataTreeDescriptor.from_dict(descriptor_dict)
        self.assertIsNotNone(descriptor)
        self.assertEqual("xyz", descriptor.data_id)
        self.assertEqual("datatree", descriptor.data_type.alias)
        self.assertEqual("EPSG:9346", descriptor.crs)
        self.assertEqual((10.0, 20.0, 30.0, 40.0), descriptor.bbox)
        self.assertEqual(20.0, descriptor.spatial_res)
        self.assertEqual(("2017-06-05", "2017-06-27"), descriptor.time_range)
        self.assertEqual("daily", descriptor.time_period)
        self.assertEqual(1, len(descriptor.coords))
        self.assertTrue("rtdt" in descriptor.coords)
        self.assertEqual(dict(x=1, y=2, z=3), descriptor.dims)
        self.assertEqual(1, len(descriptor.data_vars))
        self.assertTrue("xf" in descriptor.data_vars)
        self.assertTrue("abc" in descriptor.dataset.data_id)
        self.assertEqual("dataset", descriptor.dataset.data_type.alias)
        self.assertIn("first_inner_tree", list(descriptor.data_nodes.keys()))
        self.assertIn("first_inner_tree", descriptor.data_nodes.get("first_inner_tree").data_id)
        self.assertIn("datatree", descriptor.data_nodes.get("first_inner_tree").data_type.alias)
        self.assertIn("second_inner_tree", list(descriptor.data_nodes.keys()))
        self.assertIn("second_inner_tree", descriptor.data_nodes.get("second_inner_tree").data_id)
        self.assertIn("datatree", descriptor.data_nodes.get("second_inner_tree").data_type.alias)
        self.assertEqual(236, descriptor.attrs.get("dzus", None))
        self.assertEqual("rt5", descriptor.attrs.get("tgr7h", None))
        self.assertEqual("s8fd4w5", descriptor.attrs.get("df", None))
        self.assertEqual("object", descriptor.open_params_schema.get("type", None))

    def test_from_dict_datatree_descriptors_as_dict(self):
        descriptor_dict = dict(
            data_id="xyz",
            data_type="datatree",
            crs="EPSG:4326",
            data_nodes=dict(
                first_inner_tree=dict(
                    data_id="first_inner_tree",
                    data_type="datatree",
                )
            ),
        )
        descriptor = DataTreeDescriptor.from_dict(descriptor_dict)
        self.assertEqual("xyz", descriptor.data_id)
        self.assertEqual("datatree", descriptor.data_type.alias)
        self.assertEqual("EPSG:4326", descriptor.crs)
        self.assertEqual(1, len(descriptor.data_nodes))
        self.assertTrue("first_inner_tree" in descriptor.data_nodes)
        self.assertIsInstance(descriptor.data_nodes.get("first_inner_tree"), DataTreeDescriptor)

    def test_from_dict_datatree_descriptors_with_inner_trees_as_dict(self):
        descriptor_dict = dict(
            data_id="xyz",
            data_type="datatree",
            crs="EPSG:4326",
            dataset=dict(
                data_id="abc",
                data_type="dataset",
            ),
        )
        descriptor = DataTreeDescriptor.from_dict(descriptor_dict)
        self.assertEqual("xyz", descriptor.data_id)
        self.assertEqual("datatree", descriptor.data_type.alias)
        self.assertEqual("EPSG:4326", descriptor.crs)
        self.assertIsInstance(descriptor.dataset, DatasetDescriptor)

    def test_to_dict(self):
        # coords = dict(
        #     rtdt=VariableDescriptor(
        #         name="rtdt",
        #         dtype="rj",
        #         dims=("rtdt",),
        #         chunks=(2,),
        #         attrs=dict(ssd=6, zjgrhgu="hgtr"),
        #     )
        # )
        # var_descriptors = dict(
        #     xf=VariableDescriptor(
        #         name="xf",
        #         dtype="rj",
        #         dims=("dfjhrt", "sg"),
        #         chunks=(2, 3),
        #         attrs=dict(ssd=4, zjgrhgu="dgfrf"),
        #     )
        # )
        dataset_descriptor = DatasetDescriptor(
            data_id="xyz",
            crs="EPSG:9346",
            bbox=(10.0, 20.0, 30.0, 40.0),
            spatial_res=20.0,
            time_range=("2017-06-05", "2017-06-27"),
            time_period="daily",
            dims=dict(x=1, y=2, z=3),
            attrs=dict(dzus=236, tgr7h="rt5", df="s8fd4w5"),
        )
        inner_datatree_descriptor = DataTreeDescriptor(
            data_id="dti",
            crs="EPSG:9346",
        )
        datatree_descriptor = DataTreeDescriptor(
            data_id="dt",
            dataset=dataset_descriptor,
            data_nodes=dict(
                dti=inner_datatree_descriptor
            )
        )
        descriptor_dict = datatree_descriptor.to_dict()
        self.assertEqual(
            dict(
                data_id="dt",
                data_type="datatree",
                dataset=dict(
                    data_id="xyz",
                    data_type="dataset",
                    crs="EPSG:9346",
                    bbox=[10.0, 20.0, 30.0, 40.0],
                    spatial_res=20.0,
                    time_range=("2017-06-05", "2017-06-27"),
                    time_period="daily",
                    dims=dict(x=1, y=2, z=3),
                    attrs=dict(dzus=236, tgr7h="rt5", df="s8fd4w5")
                ),
                data_nodes=dict(
                    dti=dict(
                        data_id="dti",
                        data_type="datatree",
                        crs="EPSG:9346"
                    )
                )

            ),
            descriptor_dict,
        )
