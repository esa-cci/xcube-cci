# The MIT License (MIT)
# Copyright (c) 2025 ESA Climate Change Initiative
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

from collections.abc import Hashable, Mapping, MutableMapping
import dask
import numpy as np
from typing import Any, List, Optional
import warnings
import xarray as xr

from xcube.core.normalize import normalize_dataset
from xcube.core.store import DataType, DataTypeLike
from xcube.core.store import DataDescriptor, DatasetDescriptor, VariableDescriptor
from xcube.util.assertions import assert_true
from xcube.util.jsonschema import JsonIntegerSchema, JsonNumberSchema, JsonObjectSchema

from .cciodp import CciOdp
from .chunkstore import CciChunkStore

DATATREE_TYPE = DataType(xr.DataTree, ["datatree", "xarray.DataTree"])
DataType.register_data_type(DATATREE_TYPE)

class DataTreeDescriptor(DataDescriptor):
    """A descriptor for a tree-like hierarchical collection of gridded,
    N-dimensional datasets.

    The datasets are represented by xarray.Datasets, the collections by
    xarray.DataTrees.

    Args:
        data_id: An identifier for the data tree
        data_type: The data type of the data described
        crs: A coordinate reference system identifier, as an EPSG, PROJ
            or WKT string
        bbox: A bounding box of the data
        time_range: Start and end time delimiting this data's temporal
            extent
        time_period: The data's periodicity if it is evenly temporally
            resolved
        spatial_res: The spatial extent of a pixel in crs units
        dims: A mapping of the dataset's dimensions to their sizes
        coords: mapping of the dataset's data coordinate names to
            instances of :class:`VariableDescriptor`
        data_vars: A mapping of the dataset's variable names to
            instances of :class:`VariableDescriptor`
        dataset: A dataset associated with this node
        data_nodes: A mapping of data identifiers to DataTreeDescriptors
        attrs: A mapping containing arbitrary attributes of the dataset
        open_params_schema: A JSON schema describing the parameters that
            may be used to open this data
    """

    def __init__(
        self,
        data_id: str,
        *,
        data_type: DataTypeLike = DATATREE_TYPE,
        crs: str = None,
        bbox: tuple[float, float, float, float] = None,
        time_range: tuple[Optional[str], Optional[str]] = None,
        time_period: str = None,
        spatial_res: float = None,
        dims: Mapping[str, int] = None,
        coords: Mapping[str, "VariableDescriptor"] = None,
        data_vars: Mapping[str, "VariableDescriptor"] = None,
        dataset: DatasetDescriptor = None,
        data_nodes: Mapping[str, "DataTreeDescriptor"] = None,
        attrs: Mapping[Hashable, any] = None,
        open_params_schema: JsonObjectSchema = None,
        **additional_properties,
    ):
        super().__init__(
            data_id=data_id,
            data_type=data_type,
            crs=crs,
            bbox=bbox,
            time_range=time_range,
            time_period=time_period,
            open_params_schema=open_params_schema,
        )
        assert_true(
            DATATREE_TYPE.is_super_type_of(data_type),
            f"illegal data_type,"
            f" must be compatible with {DATATREE_TYPE!r}",
        )
        if additional_properties:
            warnings.warn(
                f"Additional properties received;"
                f" will be ignored: {additional_properties}"
            )
        self.dims = dict(dims) if dims else None
        self.spatial_res = spatial_res
        self.coords = coords if coords else None
        self.data_vars = data_vars if data_vars else None
        self.attrs = _attrs_to_json(attrs) if attrs else None
        self.dataset = dataset if dataset else None
        self.data_nodes = data_nodes if data_nodes else None

    @classmethod
    def get_schema(cls, recursion_depth: int = 0) -> JsonObjectSchema:
        schema = super().get_schema()
        schema.properties.update(
            dims=JsonObjectSchema(additional_properties=JsonIntegerSchema(minimum=0)),
            spatial_res=JsonNumberSchema(exclusive_minimum=0.0),
            coords=JsonObjectSchema(
                additional_properties=VariableDescriptor.get_schema()
            ),
            data_vars=JsonObjectSchema(
                additional_properties=VariableDescriptor.get_schema()
            ),
            dataset=DatasetDescriptor.get_schema(),
            attrs=JsonObjectSchema(additional_properties=True),
        )

        if recursion_depth < 10:
            schema.properties.update(
                data_nodes=JsonObjectSchema(
                    additional_properties=DataTreeDescriptor.get_schema(recursion_depth=recursion_depth+1)
                ),
            )
        schema.required = ["data_id", "data_type"]
        schema.additional_properties = False
        schema.factory = cls
        return schema


def _attrs_to_json(attrs: Mapping[Hashable, Any]) -> Optional[dict[str, Any]]:
    new_attrs: dict[str, Any] = {}
    for k, v in attrs.items():
        if isinstance(v, np.ndarray):
            v = v.tolist()
        elif isinstance(v, dask.array.Array):
            v = np.array(v).tolist()
        if isinstance(v, float) and np.isnan(v):
            v = None
        new_attrs[str(k)] = v
    return new_attrs


class LazyDataTree(xr.DataTree):
    def __init__(self, name=None, dataset=None, children=None):
        super().__init__(name=name, dataset=None, children={})
        self._children = children or {}


class DataTreeMapping(MutableMapping):
    def __init__(
            self,
            base_id: str,
            specifiers: List[str],
            cci_odp: CciOdp,
            normalize_data: True,
            var_names: List[str] = None,
            pattern: str = None,
            **cci_kwargs
    ):
        self._base_id = base_id
        self._specifiers = specifiers
        self._cci_odp = cci_odp
        self._normalize_data = normalize_data
        self._var_names = var_names
        self._pattern = pattern
        self._cci_kwargs = cci_kwargs
        self._loaded = {}

    def __getitem__(self, key):
        if key not in self._loaded:
            if key not in self._specifiers:
                raise ValueError(f"Unsupported key {key}")
            if self._var_names is None or self._pattern is None:
                data_id = f"{self._base_id}~{key}"
                chunk_store = CciChunkStore(self._cci_odp, data_id, self._cci_kwargs)
                ds = xr.open_zarr(chunk_store, consolidated=False)
                ds.zarr_store.set(chunk_store)
            else:
                var_datasets = []
                for var_name in self._var_names:
                    var_key = self._pattern.format(var_name=var_name, place=key)
                    data_id = f"{self._base_id}~{var_key}"
                    chunk_store = CciChunkStore(self._cci_odp, data_id, self._cci_kwargs)
                    ds = xr.open_zarr(chunk_store, consolidated=False)
                    ds.zarr_store.set(chunk_store)
                    var_datasets.append(ds)
                ds = xr.merge(var_datasets)
            if self._normalize_data:
                ds = normalize_dataset(ds)
            self._loaded[key] = xr.DataTree(name=key, dataset=ds)
        return self._loaded[key]

    def __setitem__(self, key, value):
        if isinstance(value, xr.DataTree):
            self._loaded[key] = value
        else:
            raise ValueError("Value must be DataTree")

    def __delitem__(self, key):
        self._specifiers.pop(key)
        self._loaded.pop(key, None)

    def __iter__(self):
        return iter(self._specifiers)

    def __len__(self):
        return len(self._specifiers)

    def __repr__(self):
        return f"<LazyMapping: keys={self._specifiers} loaded={list(self._loaded)}>"
