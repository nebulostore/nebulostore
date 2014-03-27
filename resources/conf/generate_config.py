#!/usr/bin/python

"""
    Script that generates xml config from Peer.xml.template (passed as stdin).
    Provide values using node names as command line argument. You can pass either
    the whole path of a variable ('peer/communication/comm-address') or only a
    part of it (ex. 'comm-address') if it is unique.
    Provide at least the following:
        ./generate_config.py\
            --app-key=123\
            --class-name=org.nebulostore.appcore.Peer\
            --configuration-class-name=org.nebulostore.appcore.PeerConfiguration\
            --bootstrap/mode=(client|server)\
            --bootstrap/address=(address)\
            --comm-cli-port=(cli_port)\
            --bootstrap-port=(bootstrap_port)\
            --tomp2p-port=(tp2p_port)\
            --bootstrap-server-tomp2p-port=(port)\
            --bdb-peer/type=(storage-holder|proxy)\
            --comm-address=(|\d+|UUID)\
            --gossip-period=(\d+)\
            --data-file=test.data

    If some flags are missing, default values from Peer.xml.template are used.

    Other flags:
        peer:
            --registration-timeout=(\d+)\
        communication.pingpong test variables:
            --pingpong-test-function=(server|client)\
            --pingpong/peer-net-address=(address)\
            --pingpong/server-net-address=(address)\
            --pingpong/peer-id=(\d+)
        communication.gossip test variables:
            --gossip/num-gossipers=(\d+)\
            --gossip/max-peers-size=(\d+)\
            --gossip/healing-factor=(\d+)\
            --gossip/swapping-factor=(\d+)\
            --gossip/cohesiveness-test-interval=(\d+[(,(\d+))]*)
"""

import sys
import re
from xml.etree import ElementTree as ET

argmap = {}

for arg in sys.argv:
    lst = arg.split('=')
    if len(lst) == 2:
        argmap[lst[0][2:]] = lst[1]

tree = ET.parse(sys.stdin)

for key, value in argmap.items():
  nodePath = './/' + re.sub('\.', '/', re.sub('_', '-', key.lower()))
  if (len(tree.findall(nodePath)) > 1):
    raise RuntimeError("Attribute not unique")
  if (len(tree.findall(nodePath)) < 1):
    raise RuntimeError("Attribute " + nodePath + " not found.")
  tree.find(nodePath).text = value

tree.write(sys.stdout)
