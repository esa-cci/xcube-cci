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

from typing import List

import lxml.etree as etree

from .constants import CCI_ODD_URL
from .sessionexecutor import SessionExecutor

ODD_NS = {
    'os': 'http://a9.com/-/spec/opensearch/1.1/',
    'param': 'http://a9.com/-/spec/opensearch/extensions/parameters/1.0/'
}

def _get_from_param_elem(param_elem: etree.Element):
    options = param_elem.findall('param:Option', namespaces=ODD_NS)
    if not options:
        return None
    if len(options) == 1:
        return options[0].get('value'), \
               int(options[0].get('label').split('(')[-1][:-1])
    return [(option.get('value'), int(option.get('label').split('(')[-1][:-1]))
            for option in options]


class OdpConnector:

    def __init__(
            self,
            user_agent: str,
            endpoint_description_url: str = CCI_ODD_URL
    ):
        self._session_executor = SessionExecutor(user_agent=user_agent)
        self._endpoint_description_url = endpoint_description_url

    def get_drs_ids(self) -> List[str]:
        meta_info_dict = self.extract_metadata_from_odd_url(self._endpoint_description_url)
        return meta_info_dict.get('drs_ids', [])

    def extract_metadata_from_odd_url(self, odd_url: str = None) -> dict:
        if not odd_url:
            return {}
        resp_content = self._session_executor.get_response_content(odd_url)
        if not resp_content:
            return {}
        return self.extract_metadata_from_odd(etree.XML(resp_content))

    @staticmethod
    def extract_metadata_from_odd(odd_xml: etree.XML) -> dict:
        metadata = {'num_files': {}}
        metadata_names = {
            'ecv': (['ecv', 'ecvs'], False),
            'frequency': (['time_frequency', 'time_frequencies'], False),
            'institute': (['institute', 'institutes'], False),
            'processingLevel': (['processing_level', 'processing_levels'], False),
            'productString': (['product_string', 'product_strings'], False),
            'productVersion': (['product_version', 'product_versions'], False),
            'dataType': (['data_type', 'data_types'], False),
            'sensor': (['sensor_id', 'sensor_ids'], False),
            'platform': (['platform_id', 'platform_ids'], False),
            'fileFormat': (['file_format', 'file_formats'], False),
            'drsId': (['drs_id', 'drs_ids'], True)
        }
        for param_elem in odd_xml.findall('os:Url/param:Parameter',
                                          namespaces=ODD_NS):
            if param_elem.attrib['name'] in metadata_names:
                element_names, add_to_num_files = \
                    metadata_names[param_elem.attrib['name']]
                param_content = _get_from_param_elem(param_elem)
                if param_content:
                    if type(param_content) is tuple:
                        metadata[element_names[0]] = param_content[0]
                        if add_to_num_files:
                            metadata['num_files'][param_content[0]] = \
                                param_content[1]
                    else:
                        names = []
                        for name, num_files in param_content:
                            names.append(name)
                            if add_to_num_files:
                                metadata['num_files'][name] = num_files
                        metadata[element_names[1]] = names
        return metadata