#!/usr/bin/python

import sys
import re
from xml.etree import ElementTree as ET

""" Merges xml files so that values from patchFile are put in file """
def patch(filename, patchFilename):
  baseTree = ET.parse(filename)
  patchTree = ET.parse(patchFilename)
  patchLeaves(baseTree, patchTree.getroot(), "")
  baseTree.write(sys.stdout)

def addByPath(baseTree, path, text):
  nodes = path[1:].split("/")
  curNode = baseTree.getroot()
  for nodeName in nodes:
    el = curNode.find(nodeName)
    if el is None:
      el = ET.SubElement(curNode, nodeName)
      curNode.append(el)
    curNode = el
  curNode.text = text

def patchLeaves(baseTree, node, path):
  if (len(list(node)) == 0):
    matchingNodes = baseTree.findall("." + path)
    if (len(matchingNodes) > 1):
      raise RuntimeError("Multiple matches")
    else:
      target = baseTree.find("." + path)
      if (target is None):
      #raise RuntimeError("Base tree node not found: <root>" + path)
        addByPath(baseTree, path, node.text)
        print("New element: " + path + " added.\n")
      else:
        target.text = node.text
  else:
    for child in node:
      patchLeaves(baseTree, child, path + "/" + child.tag)

patch(sys.argv[1], sys.argv[2])

