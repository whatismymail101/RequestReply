// pom.xml dependencies needed:
// <dependency>
//   <groupId>io.quarkus</groupId>
//   <artifactId>quarkus-smallrye-reactive-messaging-kafka</artifactId>
// </dependency>

// 1. Simple Producer - Emitting messages
package org.acme.kafka;

import io.smallrye.mutiny.Multi;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import javax.enterprise.context.ApplicationScoped;
import java.time.Duration;

@ApplicationScoped
public class PriceGenerator {

    @Outgoing("prices-out")  // Channel name
    public Multi<Integer> generate() {
        return Multi.createFrom().ticks().every(Duration.ofSeconds(5))
                .map(tick -> (int) (Math.random() * 100));
    }
}

// 2. Simple Consumer - Receiving messages
package org.acme.kafka;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PriceConsumer {

    @Incoming("prices-in")  // Channel name
    public void consume(Integer price) {
        System.out.println("Received price: " + price);
    }
}

// 3. Processor - Transform messages (consume and produce)
package org.acme.kafka;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class PriceConverter {

    @Incoming("prices-in")
    @Outgoing("prices-usd")
    public double process(Integer priceInCents) {
        return priceInCents / 100.0;
    }
}

// 4. Async Processing with CompletionStage
package org.acme.kafka;

import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Outgoing;
import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class AsyncProcessor {

    @Incoming("data-in")
    @Outgoing("data-out")
    public CompletionStage<String> processAsync(String data) {
        return CompletableFuture.supplyAsync(() -> {
            // Simulate async processing
            return data.toUpperCase();
        });
    }
}

// 5. Message Acknowledgment
package org.acme.kafka;

import io.smallrye.reactive.messaging.kafka.api.IncomingKafkaRecordMetadata;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import org.eclipse.microprofile.reactive.messaging.Message;
import javax.enterprise.context.ApplicationScoped;
import java.util.concurrent.CompletionStage;

@ApplicationScoped
public class ManualAckConsumer {

    @Incoming("manual-ack")
    public CompletionStage<Void> consume(Message<String> message) {
        String payload = message.getPayload();
        
        // Get Kafka metadata
        message.getMetadata(IncomingKafkaRecordMetadata.class)
               .ifPresent(meta -> {
                   System.out.println("Topic: " + meta.getTopic());
                   System.out.println("Partition: " + meta.getPartition());
                   System.out.println("Offset: " + meta.getOffset());
               });
        
        // Process and manually acknowledge
        System.out.println("Processing: " + payload);
        return message.ack();
    }
}

// 6. REST Endpoint producing to Kafka
package org.acme.kafka;

import io.smallrye.mutiny.Uni;
import io.smallrye.reactive.messaging.kafka.KafkaRecord;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/api/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource {

    @Inject
    @Channel("orders")
    Emitter<KafkaRecord<String, Order>> orderEmitter;

    @POST
    public Uni<String> createOrder(Order order) {
        KafkaRecord<String, Order> record = 
            KafkaRecord.of(order.getId(), order);
        
        return Uni.createFrom()
                  .completionStage(orderEmitter.send(record))
                  .map(v -> "Order sent to Kafka");
    }
}

// Order class
class Order {
    private String id;
    private String product;
    private int quantity;
    
    // Getters and setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getProduct() { return product; }
    public void setProduct(String product) { this.product = product; }
    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }
}

