
// 1. REQUEST MODEL package com.example.model;

public class OrderRequest { public String orderId; public String product; public int quantity; public String replyTo; // Topic to send reply to public String correlationId; // To correlate request with response }

// 2. RESPONSE MODEL package com.example.model;

public class OrderResponse { public String orderId; public String status; public double totalPrice; public String correlationId; }

// 3. REQUEST SENDER (Producer that expects a reply) package com.example.producer;

import com.example.model.OrderRequest; import com.example.model.OrderResponse; import io.smallrye.reactive.messaging.kafka.KafkaRecord; import io.smallrye.reactive.messaging.kafka.reply.KafkaRequestReply; import jakarta.enterprise.context.ApplicationScoped; import jakarta.inject.Inject; import org.eclipse.microprofile.reactive.messaging.Channel; import org.eclipse.microprofile.reactive.messaging.Emitter;

import java.time.Duration; import java.util.UUID; import java.util.concurrent.CompletionStage;

@ApplicationScoped public class OrderRequestSender {

@Inject
@Channel("order-requests-out")
Emitter<OrderRequest> emitter;

@Inject
KafkaRequestReply<String, OrderRequest, OrderResponse> requestReply;

// Method 1: Using KafkaRequestReply (built-in request-reply)
public CompletionStage<OrderResponse> sendOrderRequestWithReply(OrderRequest request) {
    String correlationId = UUID.randomUUID().toString();
    
    return requestReply.request(
        KafkaRecord.of(correlationId, request),
        Duration.ofSeconds(30) // Timeout for reply
    ).thenApply(response -> response.getPayload());
}

// Method 2: Manual request-reply pattern
public void sendOrderRequest(String orderId, String product, int quantity) {
    OrderRequest request = new OrderRequest();
    request.orderId = orderId;
    request.product = product;
    request.quantity = quantity;
    request.replyTo = "order-responses"; // Reply topic
    request.correlationId = UUID.randomUUID().toString();
    
    emitter.send(request);
}
}

// 4. REQUEST PROCESSOR (Consumer that sends reply) package com.example.consumer;

import com.example.model.OrderRequest; import com.example.model.OrderResponse; import io.smallrye.reactive.messaging.kafka.KafkaRecord; import io.smallrye.reactive.messaging.kafka.reply.KafkaReply; import jakarta.enterprise.context.ApplicationScoped; import org.eclipse.microprofile.reactive.messaging.Incoming; import org.eclipse.microprofile.reactive.messaging.Outgoing; import org.jboss.logging.Logger;

@ApplicationScoped public class OrderProcessor {

private static final Logger LOG = Logger.getLogger(OrderProcessor.class);

// Method 1: Using @KafkaReply annotation (automatic reply)
@Incoming("order-requests-in")
@KafkaReply
public OrderResponse processOrder(OrderRequest request) {
    LOG.infof("Processing order: %s for product: %s", 
              request.orderId, request.product);
    
    // Business logic here
    double price = calculatePrice(request.product, request.quantity);
    
    OrderResponse response = new OrderResponse();
    response.orderId = request.orderId;
    response.status = "CONFIRMED";
    response.totalPrice = price;
    response.correlationId = request.correlationId;
    
    return response;
}

// Method 2: Manual reply pattern
@Incoming("order-requests-manual")
@Outgoing("order-responses-out")
public KafkaRecord<String, OrderResponse> processOrderManual(
        KafkaRecord<String, OrderRequest> record) {
    
    OrderRequest request = record.getPayload();
    LOG.infof("Processing order manually: %s", request.orderId);
    
    OrderResponse response = new OrderResponse();
    response.orderId = request.orderId;
    response.status = "CONFIRMED";
    response.totalPrice = calculatePrice(request.product, request.quantity);
    response.correlationId = request.correlationId;
    
    // Send reply with same correlation key
    return KafkaRecord.of(request.correlationId, response);
}

private double calculatePrice(String product, int quantity) {
    return quantity * 29.99; // Simple pricing
}
}

// 5. RESPONSE HANDLER (Consumer that receives replies) package com.example.consumer;

import com.example.model.OrderResponse; import jakarta.enterprise.context.ApplicationScoped; import org.eclipse.microprofile.reactive.messaging.Incoming; import org.jboss.logging.Logger;

@ApplicationScoped public class OrderResponseHandler {

private static final Logger LOG = Logger.getLogger(OrderResponseHandler.class);

@Incoming("order-responses-in")
public void handleOrderResponse(OrderResponse response) {
    LOG.infof("Received response for order %s: %s - $%.2f", 
              response.orderId, 
              response.status, 
              response.totalPrice);
    
    // Handle the response (update database, notify user, etc.)
}
}

// 6. REST ENDPOINT TO TRIGGER REQUEST package com.example.resource;

import com.example.model.OrderRequest; import com.example.model.OrderResponse; import com.example.producer.OrderRequestSender; import jakarta.inject.Inject; import jakarta.ws.rs.*; import jakarta.ws.rs.core.MediaType;

import java.util.concurrent.CompletionStage;

@Path("/orders") @Produces(MediaType.APPLICATION_JSON) @Consumes(MediaType.APPLICATION_JSON) public class OrderResource {

@Inject
OrderRequestSender orderRequestSender;

@POST
@Path("/request-reply")
public CompletionStage<OrderResponse> createOrderWithReply(OrderRequest request) {
    return orderRequestSender.sendOrderRequestWithReply(request);
}

@POST
@Path("/async")
public String createOrderAsync(OrderRequest request) {
    orderRequestSender.sendOrderRequest(
        request.orderId, 
        request.product, 
        request.quantity
    );
    return "Order request sent: " + request.orderId;
}
}
