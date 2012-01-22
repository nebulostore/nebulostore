package org.nebulostore.query.functions.dql;

import java.util.LinkedList;
import java.util.List;

import org.antlr.runtime.RecognitionException;
import org.nebulostore.query.functions.CallParametersConditions;
import org.nebulostore.query.functions.DQLFunction;
import org.nebulostore.query.language.interpreter.datatypes.BooleanValue;
import org.nebulostore.query.language.interpreter.datatypes.IDQLValue;
import org.nebulostore.query.language.interpreter.datatypes.LambdaValue;
import org.nebulostore.query.language.interpreter.datatypes.ListValue;
import org.nebulostore.query.language.interpreter.exceptions.InterpreterException;

public class Filter extends DQLFunction {
  private static CallParametersConditions conditions_ = CallParametersConditions
      .newBuilder().build();

  public Filter() {
    super("filter", conditions_);
  }

  @Override
  public IDQLValue call(List<IDQLValue> params) throws InterpreterException,
      RecognitionException {
    checkParams(params);
    ListValue inputList = ((ListValue) params.get(1));
    ListValue ret = new ListValue(inputList.getType(),
        inputList.getPrivacyLevel());

    for (IDQLValue element : inputList) {
      List<IDQLValue> lambdaParams = new LinkedList<IDQLValue>();
      lambdaParams.add(element);
      if (((BooleanValue) (((LambdaValue) params.get(0)).evaluate(lambdaParams)))
          .getValue()) {
        ret.addNum(element);
      }
    }

    return ret;
  }

  @Override
  public String help() {
    return "FILTER : (LAMBDA : Type -> Boolean), List<Type> -> List<Type>";
  }
}
