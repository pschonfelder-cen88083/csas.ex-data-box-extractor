import requests
import xml.etree.ElementTree as ET
import io

namespace = 'http://seznam.gov.cz/ovm/datafile/seznam_ds/v1'
ET.register_namespace('', namespace)
ns = '{' + namespace + '}'

# usage example:
# run:
#   load_po(dataWriter)
#   load_pfo(print)
#   load_ovm(dataWriter)

def url_data_dtream_as_file(url):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        return io.BytesIO(response.content)
    else:
        raise Exception(f'Failed to download url:{url} response {response.status_code}')


def get_data_columns() -> list:
    return [string.upper() for string in get_exact_xml_names()]


def get_exact_xml_names() -> list:
    return ['id', 'type', 'subtype', 'tradeName', 'ico', 'isMaster', 'masterId']


def element_to_attr_name() -> dict:
    """ XML element name (with namespace) to result column name  """
    mapping = {}
    for col_name in get_exact_xml_names():
        mapping[ns + col_name] = col_name.upper()
    return mapping


def load_data(file, data_writer) -> int:
    """ data_writer is function processing dict parameter"""
    mapping: dict = element_to_attr_name()
    count = 0
    box_attributes = {}
    for event, elem in ET.iterparse(file):
        if elem.tag == ns + 'box':
            data_writer(box_attributes)
            box_attributes = {}  # Reset for next box
            count = count + 1
        else:
            if elem.tag in mapping:
                colum_name = mapping[elem.tag]
                box_attributes[colum_name] = elem.text
        elem.clear()  # uvolňuje paměť
    return count


def load_po(data_writer) -> int:
    # Právnické osoby	https://www.mojedatovaschranka.cz/sds/datafile?format=xml&service=seznam_ds_po	653 MB
    url_po = "https://www.mojedatovaschranka.cz/sds/datafile?format=xml&service=seznam_ds_po"
    with url_data_dtream_as_file(url_po) as file:
        count = load_data(file=file, data_writer=data_writer)
        print(f"Load PO {url_po} count {count}")
        return count


def load_pfo(data_writer) -> int:
    # Podnikající fyzické osoby	https://www.mojedatovaschranka.cz/sds/datafile?format=xml&service=seznam_ds_pfo	5 MB
    url_pfo = "https://www.mojedatovaschranka.cz/sds/datafile?format=xml&service=seznam_ds_pfo"
    with url_data_dtream_as_file(url_pfo) as file:
        count = load_data(file=file, data_writer=data_writer)
        print(f"Load PFO {url_pfo} count {count}")
        return count


def load_ovm(data_writer) -> int:
    # Orgány veřejné moci	https://www.mojedatovaschranka.cz/sds/datafile?format=xml&service=seznam_ds_ovm	16 MB
    url_ovm = "https://www.mojedatovaschranka.cz/sds/datafile?format=xml&service=seznam_ds_ovm"
    with url_data_dtream_as_file(url_ovm) as file:
        count = load_data(file=file, data_writer=data_writer)
        print(f"Load OWM {url_ovm} count {count}")
        return count

