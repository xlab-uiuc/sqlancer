import xml.etree.ElementTree as ET
from xml.dom import minidom
import os
import random

conf_options = {
    "hive.default.serde": ["org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"], # TODO
    "hive.lazysimple.extended_boolean_literal": ["true", "false"],
    # "hive.io.exception.handlers": [],
    "hive.input.format": ["org.apache.hadoop.hive.ql.io.CombineHiveInputFormat", "org.apache.hadoop.hive.ql.io.HiveInputFormat"],
    "hive.default.fileformat": ["TextFile", "SequenceFile", "RCfile", "ORC", "Parquet"],
}
NUM_SPARK = 2

for i in range(NUM_SPARK):
    c = ET.Element('configuration')
    for k in conf_options.keys():
        p = ET.SubElement(c, 'property')
        n = ET.SubElement(p, 'name')
        n.text = k
        v = ET.SubElement(p, 'value')
        v.text = random.choice(conf_options[k])

    xmlstr = minidom.parseString(ET.tostring(c)).toprettyxml(indent="   ")
    xmlstr = xmlstr[:22] + '\n<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>' + xmlstr[22:]
    with open('../spark'+str(i)+'/hive-site.xml', 'w') as f:
        f.write(xmlstr)