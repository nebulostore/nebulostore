package org.nebulostore.query.functions.ql.xml;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nebulostore.query.executor.ExecutorContext;
import org.nebulostore.query.functions.CallParametersConditions;
import org.nebulostore.query.functions.DQLFunction;
import org.nebulostore.query.functions.exceptions.FunctionCallException;
import org.nebulostore.query.language.interpreter.datatypes.DQLPrimitiveType;
import org.nebulostore.query.language.interpreter.datatypes.DQLPrimitiveType.DQLPrimitiveTypeEnum;
import org.nebulostore.query.language.interpreter.datatypes.DQLType;
import org.nebulostore.query.language.interpreter.datatypes.values.BooleanValue;
import org.nebulostore.query.language.interpreter.datatypes.values.IDQLValue;
import org.nebulostore.query.language.interpreter.datatypes.values.IntegerValue;
import org.nebulostore.query.language.interpreter.datatypes.values.ListValue;
import org.nebulostore.query.language.interpreter.datatypes.values.StringValue;
import org.nebulostore.query.privacy.level.PrivateMy;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public class XPath extends DQLFunction {

  private static Log log = LogFactory.getLog(XPath.class);

  private static CallParametersConditions conditions_ = CallParametersConditions
      .newBuilder()
      .parameter(0, new DQLPrimitiveType(DQLPrimitiveTypeEnum.DQLString))
      .parameter(1, new DQLPrimitiveType(DQLPrimitiveTypeEnum.DQLString))
      .parameter(2, new DQLPrimitiveType(DQLPrimitiveTypeEnum.DQLBoolean))
      .parametersNumber(3).build();

  public XPath(ExecutorContext context) {
    super("xpath", conditions_, context);
  }

  @Override
  public IDQLValue call(List<IDQLValue> params) throws FunctionCallException {
    checkParams(params);

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder;
    try {
      builder = factory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      log.error(e);
      throw new FunctionCallException(e);
    }
    Document doc;
    try {
      doc = builder.parse(new ByteArrayInputStream(
          ((StringValue) params.get(1)).getValue().getBytes()));
    } catch (SAXException e) {
      log.error(e);
      throw new FunctionCallException(e);
    } catch (IOException e) {
      log.error(e);
      throw new FunctionCallException(e);
    }

    XPathFactory xPathFactory = XPathFactory.newInstance();
    javax.xml.xpath.XPath xpath = xPathFactory.newXPath();
    XPathExpression expression;
    try {
      expression = xpath.compile(((StringValue) params.get(0)).getValue());
    } catch (XPathExpressionException e) {
      log.error(e);
      throw new FunctionCallException(e);
    }
    NodeList results = null;
    try {
      results = (NodeList) expression.evaluate(doc, XPathConstants.NODESET);
    } catch (XPathExpressionException e) {
      log.error(e);
      throw new FunctionCallException(e);
    }

    boolean forceList = ((BooleanValue) params.get(2)).getValue();

    return serializeNodeList(results, forceList);
  }

  private static IDQLValue serializeNodeList(NodeList list, boolean forceList) {
    if ((!forceList) && list.getLength() == 1) {
      return serializePrimitive(list.item(0).getTextContent());
    } else {
      // TODO: Proper support of privacy levels here
      // TODO: Empty list ? What about its privacy level?

      DQLType firstType = serializePrimitive(list.item(0).getTextContent())
          .getType();

      ListValue ret = new ListValue(firstType, PrivateMy.getInstance());
      for (int i = 0; i < list.getLength(); i++) {
        ret.add(serializePrimitive(list.item(i).getTextContent()));
      }

      return ret;
    }

  }

  private static IDQLValue serializePrimitive(String textContent) {
    try {
      // TODO: Proper support of privacy levels here
      return new IntegerValue(Integer.parseInt(textContent),
          PrivateMy.getInstance());
    } catch (Exception e) {
    }
    // TODO: Proper support of privacy levels here
    return new StringValue(textContent, PrivateMy.getInstance());
  }

  @Override
  public String help() {
    throw new NotImplementedException();
  }

}