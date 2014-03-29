/**
 * Addressing primitives for users and data objects.<br />
 *
 * Every object (NebuloObject) stored in the system is identified by<br />
 * <pre>    NebuloAddress = (AppKey, ObjectId)</pre>
 * {@link org.nebulostore.appcore.addressing.AppKey} identifies a single user in the system while
 * {@link org.nebulostore.appcore.addressing.ObjectId} identifies his piece of data.
 * User can own multiple instances of NebuloStore (e.g. desktop and lightweight mobile)
 * and every running instance is identified by
 * {@link org.nebulostore.communication.naming.CommAddress}.<br />
 * Note that with such addressing we are able to communicate with NebuloStore instance using
 * CommAddress not knowing the identity of its owner (AppKey).
 *
 * @see org.nebulostore.appcore.modules.Module
 * @see org.nebulostore.appcore.messaging.Message
 * @see org.nebulostore.appcore.messaging.MessageVisitor
 */
package org.nebulostore.appcore.addressing;
