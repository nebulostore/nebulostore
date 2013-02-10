package org.nebulostore.timer;

import org.nebulostore.appcore.JobModule;
import org.nebulostore.appcore.MessageVisitor;
import org.nebulostore.appcore.exceptions.NebuloException;


/**
 * Test initialization message.
 */
public class InitSimpleTimerTestMessage extends AbstractTimerTestMessage {
  private static final long serialVersionUID = 1714324074362911323L;
  public SimpleTimerTestModule handler_ = new SimpleTimerTestModule();

  public InitSimpleTimerTestMessage(String jobId) {
    super(jobId);
  }

  public JobModule getHandler() throws NebuloException {
    return handler_;
  }

  @Override
  public <R> R accept(MessageVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }

  @Override
  public <R> R accept(TimerTestVisitor<R> visitor) throws NebuloException {
    return visitor.visit(this);
  }
}
