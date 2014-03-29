package org.nebulostore.broker;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;
import org.nebulostore.communication.naming.CommAddress;

import static org.junit.Assert.assertTrue;

/**
 * @author Jadwiga Kanska
 */
public class ContractTest {
  private static final int SIZE = 10 * 1024;

  @Test
  public void testUniqueId() {

    Contract contract1 = new Contract(CommAddress.newRandomCommAddress(),
        CommAddress.newRandomCommAddress(), SIZE);
    Contract contract2 = new Contract(CommAddress.newRandomCommAddress(),
        CommAddress.newRandomCommAddress(), SIZE);

    Set<Contract> contracts = new HashSet<>();

    contracts.add(contract1);
    assertTrue(contracts.size() == 1);
    assertTrue(contracts.contains(contract1));

    contracts.add(contract2);
    assertTrue(contracts.size() == 2);
    assertTrue(contracts.contains(contract2));

  }
}
