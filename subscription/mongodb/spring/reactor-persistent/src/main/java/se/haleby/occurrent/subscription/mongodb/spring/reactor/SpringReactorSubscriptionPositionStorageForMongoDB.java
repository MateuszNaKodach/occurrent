package se.haleby.occurrent.subscription.mongodb.spring.reactor;

import com.mongodb.client.result.UpdateResult;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.springframework.data.mongodb.core.ReactiveMongoOperations;
import org.springframework.data.mongodb.core.query.Update;
import reactor.core.publisher.Mono;
import se.haleby.occurrent.subscription.SubscriptionPosition;
import se.haleby.occurrent.subscription.api.reactor.ReactorSubscriptionPositionStorage;
import se.haleby.occurrent.subscription.mongodb.MongoDBOperationTimeBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.MongoDBResumeTokenBasedSubscriptionPosition;
import se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons;

import static java.util.Objects.requireNonNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCloudEventsToJsonDeserializer.ID;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.generateOperationTimeStreamPositionDocument;
import static se.haleby.occurrent.subscription.mongodb.internal.MongoDBCommons.generateResumeTokenStreamPositionDocument;

/**
 * A Spring implementation of {@link ReactorSubscriptionPositionStorage} that stores {@link SubscriptionPosition} in MongoDB.
 */
public class SpringReactorSubscriptionPositionStorageForMongoDB implements ReactorSubscriptionPositionStorage {

    private final ReactiveMongoOperations mongo;
    private final String streamPositionCollection;

    /**
     * Create a new instance of {@link SpringReactorSubscriptionPositionStorageForMongoDB}
     *
     * @param mongo                    The {@link ReactiveMongoOperations} implementation to use persisting subscription positions to MongoDB.
     * @param streamPositionCollection The collection that will contain the subscription position for each subscriber.
     */
    public SpringReactorSubscriptionPositionStorageForMongoDB(ReactiveMongoOperations mongo, String streamPositionCollection) {
        requireNonNull(mongo, ReactiveMongoOperations.class.getSimpleName() + " cannot be null");
        requireNonNull(streamPositionCollection, "streamPositionCollection cannot be null");
        this.mongo = mongo;
        this.streamPositionCollection = streamPositionCollection;
    }

    @Override
    public Mono<SubscriptionPosition> write(String subscriptionId, SubscriptionPosition changeStreamPosition) {
        Mono<?> result;
        if (changeStreamPosition instanceof MongoDBResumeTokenBasedSubscriptionPosition) {
            result = persistResumeTokenStreamPosition(subscriptionId, ((MongoDBResumeTokenBasedSubscriptionPosition) changeStreamPosition).resumeToken);
        } else if (changeStreamPosition instanceof MongoDBOperationTimeBasedSubscriptionPosition) {
            result = persistOperationTimeStreamPosition(subscriptionId, ((MongoDBOperationTimeBasedSubscriptionPosition) changeStreamPosition).operationTime);
        } else {
            String streamPositionString = changeStreamPosition.asString();
            Document document = MongoDBCommons.generateGenericStreamPositionDocument(subscriptionId, streamPositionString);
            result = persistDocumentStreamPosition(subscriptionId, document);
        }
        return result.thenReturn(changeStreamPosition);
    }

    @Override
    public Mono<Void> delete(String subscriptionId) {
        return mongo.remove(query(where(ID).is(subscriptionId)), streamPositionCollection).then();
    }

    private Mono<Document> persistResumeTokenStreamPosition(String subscriptionId, BsonDocument resumeToken) {
        Document document = generateResumeTokenStreamPositionDocument(subscriptionId, resumeToken);
        return persistDocumentStreamPosition(subscriptionId, document).thenReturn(document);
    }

    private Mono<Document> persistOperationTimeStreamPosition(String subscriptionId, BsonTimestamp timestamp) {
        Document document = generateOperationTimeStreamPositionDocument(subscriptionId, timestamp);
        return persistDocumentStreamPosition(subscriptionId, document).thenReturn(document);
    }

    private Mono<UpdateResult> persistDocumentStreamPosition(String subscriptionId, Document document) {
        return mongo.upsert(query(where(ID).is(subscriptionId)),
                Update.fromDocument(document),
                streamPositionCollection);
    }

    @Override
    public Mono<SubscriptionPosition> read(String subscriptionId) {
        return mongo.findOne(query(where(ID).is(subscriptionId)), Document.class, streamPositionCollection)
                .map(MongoDBCommons::calculateSubscriptionPositionFromMongoStreamPositionDocument);
    }
}