package eu.arrowhead.common.api.clients.core;

import eu.arrowhead.common.api.ArrowheadConverter;
import eu.arrowhead.common.api.ArrowheadSecurityContext;
import eu.arrowhead.common.api.clients.HttpClient;
import eu.arrowhead.common.api.clients.OrchestrationStrategy;
import eu.arrowhead.common.exception.ArrowheadRuntimeException;
import eu.arrowhead.common.misc.ArrowheadProperties;
import eu.arrowhead.common.model.ArrowheadSystem;
import eu.arrowhead.common.model.Event;
import eu.arrowhead.common.model.EventFilter;
import eu.arrowhead.common.model.PublishEvent;

import javax.ws.rs.core.UriBuilder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A client for interacting with the Event Handler system. See the static create* methods for how to get an instance of
 * one of these.
 */
public class EventHandlerClient extends HttpClient {
    private static final Map<EventHandlerClient, Map<ArrowheadSystem, Set<String>>> subscriptions = new HashMap<>();
    private static final UriBuilder PUBLISH_URI = UriBuilder.fromPath("publish");
    private static final UriBuilder SUBSCRIPTION_URI = UriBuilder.fromPath("subscription");
    private String feedbackUri;
    private boolean feedback;

    /**
     * Create a new client from the settings in the default properties files.
     * @param securityContext the security context to use.
     * @return your shiny new client.
     */
    public static EventHandlerClient createFromProperties(ArrowheadSecurityContext securityContext) {
        return createFromProperties(ArrowheadProperties.loadDefault(), securityContext);
    }

    /**
     * Create a new client from given set of properties.
     * @param securityContext the security context to use.
     * @return your shiny new client.
     */
    public static EventHandlerClient createFromProperties(ArrowheadProperties props, ArrowheadSecurityContext securityContext) {
        final boolean secure = props.isSecure();
        return new EventHandlerClient(secure, securityContext, props.getEhAddress(),props.getEhPort(),
                props.getFeedbackUri(), props.getFeedback());
    }

    /**
     * Create a new client using default values.
     * @param securityContext the security context to use.
     * @return your shiny new client.
     */
    public static EventHandlerClient createDefault(ArrowheadSecurityContext securityContext) {
        final boolean isSecure = ArrowheadProperties.getDefaultIsSecure();
        return new EventHandlerClient(isSecure, securityContext, ArrowheadProperties.getDefaultEhAddress(),
                ArrowheadProperties.getDefaultEhPort(isSecure), ArrowheadProperties.getDefaultFeedbackUri(),
                ArrowheadProperties.getDefaultFeedback());
    }

    /**
     * Private construct, see the create* methods.
     * @param secure use secure mode?
     * @param securityContext the security context to use.
     * @param host the host.
     * @param port the port.
     */
    private EventHandlerClient(boolean secure, ArrowheadSecurityContext securityContext, String host, int port,
                               String feedbackUri, boolean feedback) {
        super(new OrchestrationStrategy.Never(secure, host, port, "eventhandler",
                ArrowheadConverter.JSON), securityContext);
        this.feedbackUri = feedbackUri;
        this.feedback = feedback;
    }

    /**
     * Publish a new event.
     * @param event the event.
     * @param eventSource the source of the event.
     */
    public void publish(Event event, ArrowheadSystem eventSource) {
        PublishEvent eventPublishing = new PublishEvent(eventSource, event, feedbackUri);
        request(Method.POST, PUBLISH_URI, eventPublishing);
        log.info("Event published to EH.");
    }

    /**
     * Subscribe to an event type, using notify path from the default properties files (or the default one if not set).
     * @param eventType the event type to subscribe to.
     * @param consumer the consumer of the events.
     */
    public void subscribe(String eventType, ArrowheadSystem consumer) {
        subscribe(eventType, consumer, ArrowheadProperties.loadDefault());
    }

    /**
     * Subscribe to an event type, using notify path from the given properties (or the default one if not set).
     * @param eventType the event type to subscribe to.
     * @param consumer the consumer of the events.
     * @param props the properties.
     */
    public void subscribe(String eventType, ArrowheadSystem consumer, ArrowheadProperties props) {
        subscribe(eventType, consumer, props.getNotifyUri());
    }

    /**
     * Subscribe to an event type, using the given notify path.
     * @param eventType the event type to subscribe to.
     * @param consumer the consumer of the events.
     * @param notifyPath the notify path.
     */
    public void subscribe(String eventType, ArrowheadSystem consumer, String notifyPath) {
        Map<ArrowheadSystem, Set<String>> handlerSubscriptions = subscriptions.get(this);
        final Set<String> consumerSubscriptions = handlerSubscriptions != null ? handlerSubscriptions.get(consumer) : null;

        if (consumerSubscriptions != null && consumerSubscriptions.contains(eventType))
            throw new ArrowheadRuntimeException("Already subscribed to " + eventType);

        EventFilter filter = new EventFilter(eventType, consumer, notifyPath);
        request(Method.POST, SUBSCRIPTION_URI, filter);

        if (!subscriptions.containsKey(this)) subscriptions.put(this, new HashMap<>());
        handlerSubscriptions = subscriptions.get(this);
        if (!handlerSubscriptions.containsKey(consumer)) handlerSubscriptions.put(consumer, new HashSet<>());
        handlerSubscriptions.get(consumer).add(eventType);

        log.info("Subscribed to " + eventType + " event types.");
    }

    /**
     * Unsubscribe to a single event type.
     * @param consumer the consumer.
     * @param eventType the event type.
     */
    public void unsubscribe(ArrowheadSystem consumer, String eventType) {
        String consumerName = consumer.getSystemName();

        request(Method.DELETE, SUBSCRIPTION_URI.clone().path("type").path(eventType).path("consumer").path(consumerName));

        final Map<ArrowheadSystem, Set<String>> handlerSubscriptions = subscriptions.get(this);
        final Set<String> consumerSubscriptions = handlerSubscriptions != null ? handlerSubscriptions.get(consumer) : null;
        if (consumerSubscriptions != null) consumerSubscriptions.remove(eventType);

        log.info("Unsubscribed from " + eventType + " event types.");
    }

    /**
     * Unsubscribe to a set of events.
     * @param consumer the consumer.
     * @param subscriptions the events.
     */
    public void unsubscribe(ArrowheadSystem consumer, Set<String> subscriptions) {
        subscriptions.forEach(eventType -> this.unsubscribe(consumer, eventType));
    }

    /**
     * Unsubscribe many event from several systems
     * @param subscriptions subscriptions to unsubscribe from.
     */
    public void unsubscribe(Map<ArrowheadSystem, Set<String>> subscriptions) {
        subscriptions.forEach(this::unsubscribe);
    }

    /**
     * Unsubscribe everything that was ever subscribe to. Note this will be called automatically by
     * {@link eu.arrowhead.common.api.ArrowheadApplication} if this class is used.
     */
    public static void unsubscribeAll() {
        subscriptions.forEach(EventHandlerClient::unsubscribe);
    }

    public String getFeedbackUri() {
        return feedbackUri;
    }

    public EventHandlerClient setFeedbackUri(String feedbackUri) {
        this.feedbackUri = feedbackUri;
        return this;
    }

    public boolean isFeedback() {
        return feedback;
    }

    public EventHandlerClient setFeedback(boolean feedback) {
        this.feedback = feedback;
        return this;
    }
}