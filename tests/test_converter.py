import xml.etree.ElementTree as ET
from undatum.cmds.converter import etree_to_dict

def test_etree_to_dict_simple():
    xml_data = "<root><item>value</item></root>"
    root = ET.fromstring(xml_data)
    result = etree_to_dict(root)
    assert result == {'root': {'item': 'value'}}

def test_etree_to_dict_nested():
    xml_data = "<root><item><subitem>value</subitem></item></root>"
    root = ET.fromstring(xml_data)
    result = etree_to_dict(root)
    assert result == {'root': {'item': {'subitem': 'value'}}}

def test_etree_to_dict_list():
    xml_data = "<root><item>1</item><item>2</item></root>"
    root = ET.fromstring(xml_data)
    result = etree_to_dict(root)
    assert result == {'root': {'item': ['1', '2']}}

def test_etree_to_dict_attributes():
    xml_data = '<root id="1"><item>value</item></root>'
    root = ET.fromstring(xml_data)
    result = etree_to_dict(root)
    assert result == {'root': {'@id': '1', 'item': 'value'}}
