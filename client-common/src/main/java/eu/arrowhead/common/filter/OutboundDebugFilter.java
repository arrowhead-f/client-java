/*
 *  Copyright (c) 2018 AITIA International Inc.
 *
 *  This work is part of the Productive 4.0 innovation project, which receives grants from the
 *  European Commissions H2020 research and innovation programme, ECSEL Joint Undertaking
 *  (project no. 737459), the free state of Saxony, the German Federal Ministry of Education and
 *  national funding authorities from involved countries.
 */

package eu.arrowhead.common.filter;

import eu.arrowhead.common.api.ArrowheadConverter;
import org.apache.log4j.Logger;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.ext.Provider;

@Provider
@Priority(Priorities.USER)
public class OutboundDebugFilter implements ContainerResponseFilter {
  protected final Logger log = Logger.getLogger(getClass());

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) {
    if (Boolean.valueOf(System.getProperty("debug_mode", "false"))) {
      if (responseContext.getEntity() != null) {
        log.info("Response to the request at: " + requestContext.getUriInfo().getRequestUri().toString());
        log.info(ArrowheadConverter.json().toString(responseContext.getEntity()));
      }
    }
  }
}
