package in.succinct.catalog.indexer.ingest;

import com.venky.cache.Cache;
import com.venky.core.string.StringUtil;
import com.venky.core.util.Bucket;
import com.venky.core.util.ObjectUtil;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Circle;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment.FulfillmentType;
import in.succinct.beckn.FulfillmentStop;
import in.succinct.beckn.Fulfillments;
import in.succinct.beckn.Images;
import in.succinct.beckn.Intent;
import in.succinct.beckn.Item;
import in.succinct.beckn.ItemQuantity;
import in.succinct.beckn.Items;
import in.succinct.beckn.Location;
import in.succinct.beckn.Locations;
import in.succinct.beckn.Message;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Request;
import in.succinct.beckn.Scalar;
import in.succinct.beckn.Subscriber;
import in.succinct.beckn.Time;
import in.succinct.catalog.indexer.db.model.Fulfillment;
import in.succinct.catalog.indexer.db.model.IndexedSubscriberModel;
import in.succinct.catalog.indexer.db.model.Payment;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import in.succinct.json.JSONAwareWrapper;
import org.apache.lucene.search.Query;
import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;

@SuppressWarnings("unused")
public class CatalogSearchEngine {
    Subscriber bpp;
    public CatalogSearchEngine(Subscriber bpp){
        this.bpp = bpp;
    }
    public void search(Request request, Request reply) {
        try{
            indexed_search(request,reply);
        }catch (Exception ex){
            Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Exception found", ex);
        }
    }

    public Subscriber getSubscriber() {
        return bpp;
    }

    private void indexed_search(Request request, Request reply) {
        //request.getContext().
        reply.setMessage(new Message());
        Subscriber subscriber = getSubscriber();
        Catalog catalog = new Catalog();
        catalog.setDescriptor(new Descriptor());

        reply.getMessage().setCatalog(catalog);
        Providers providers = new Providers();
        catalog.setProviders(providers);
        catalog.setFulfillments(new Fulfillments());

        Message message  = request.getMessage();
        Intent intent = message.getIntent();
        if (intent.isIncrementalRequestStartTrigger()){
            intent.setStartTime(request.getContext().getTimestamp());
        }else if (intent.isIncrementalRequestEndTrigger()){
            intent.setEndTime(request.getContext().getTimestamp());
        }
        in.succinct.beckn.Fulfillment intentFulfillment = intent.getFulfillment();

        Descriptor intentDescriptor = normalizeDescriptor(intent.getDescriptor()) ;

        Provider provider = intent.getProvider();
        Descriptor providerDescriptor = provider == null ? intentDescriptor : normalizeDescriptor(provider.getDescriptor());



        Item item = intent.getItem();
        Descriptor itemDescriptor = item == null ? intentDescriptor : normalizeDescriptor(item.getDescriptor());

        Category category = intent.getCategory();
        Descriptor categoryDescriptor = category == null ? intentDescriptor : normalizeDescriptor(category.getDescriptor());

        StringBuilder q = new StringBuilder();
        if (providerDescriptor != null && !ObjectUtil.isVoid(providerDescriptor.getName())){
            providerDescriptor.setName(providerDescriptor.getName().trim());
            q.append(String.format("     ( PROVIDER:%s* or PROVIDER_LOCATION:%s* )",providerDescriptor.getName(),providerDescriptor.getName()));
        }else if (provider != null && !ObjectUtil.isVoid(provider.getId())) {
            in.succinct.catalog.indexer.db.model.Provider dbProvider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).newRecord();
            dbProvider.setSubscriberId(getSubscriber().getSubscriberId());
            dbProvider.setObjectId(provider.getId());
            dbProvider = Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).getRefreshed(dbProvider);
            if (dbProvider.getRawRecord().isNewRecord()){
                q.append(String.format(" ( PROVIDER:%s or PROVIDER_LOCATION:%s )", provider.getId(), provider.getId()));
            }else {
                q.append(String.format(" ( PROVIDER_ID:%d or PROVIDER_LOCATION_ID:%d )", dbProvider.getId(), dbProvider.getId()));
            }
        }
        if (categoryDescriptor != null && !ObjectUtil.isVoid(categoryDescriptor.getName())){
            categoryDescriptor.setName(categoryDescriptor.getName().trim());
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(String.format(" CATEGORY:%s* ",categoryDescriptor.getName()));
        }else if (category != null && !ObjectUtil.isVoid(category.getId())){
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            category.setId(category.getId().trim());
            q.append(String.format(" CATEGORY:%s* ",category.getId()));
        }
        if (itemDescriptor != null && !ObjectUtil.isVoid(itemDescriptor.getName())){
            itemDescriptor.setName(itemDescriptor.getName().trim());
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(String.format(" OBJECT_NAME:%s* ",itemDescriptor.getName()));
        }
        List<Long> itemIds = new ArrayList<>();
        if (!ObjectUtil.isVoid(q.toString())) {
            LuceneIndexer indexer = LuceneIndexer.instance(in.succinct.catalog.indexer.db.model.Item.class);
            Query query = indexer.constructQuery(q.toString());
            Config.instance().getLogger(getClass().getName()).info("Searching for /items/search/" + q);
            itemIds = indexer.findIds(query, 0);
            Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Query result size: " + itemIds.size());
            if (itemIds.isEmpty()) {
                reply.setSuppressed(true);
                return;
                // Empty provider list.
            }
        }


        Select sel = new Select().from(in.succinct.catalog.indexer.db.model.Item.class);
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        where.add(new Expression(sel.getPool(), "ACTIVE", Operator.EQ, true));
        where.add(new Expression(sel.getPool(),"SUBSCRIBER_ID", Operator.EQ, getSubscriber().getSubscriberId()));

        if (!itemIds.isEmpty()){
            where.add(Expression.createExpression(sel.getPool(),"ID",Operator.IN,itemIds.toArray()));
        }

        sel.where(where).add(String.format(" and provider_location_id in ( select id from provider_locations where provider_id in (select id from providers where subscriber_id = '%s'))",
                getSubscriber().getSubscriberId())     );

        List<in.succinct.catalog.indexer.db.model.Item> records = sel.where(where).execute(in.succinct.catalog.indexer.db.model.Item.class, 30);

        Bucket numItemsReturned = new Bucket();
        Set<String> subscriberIds = new HashSet<>();
        Set<Long> providerIds = new HashSet<>();
        Set<Long> providerLocationIds = new HashSet<>();
        Set<Long> fulfillmentIds = new HashSet<>();
        Set<Long> categoryIds = new HashSet<>();
        Set<Long> paymentIds = new HashSet<>();

        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Item>> appItemMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Item.class,new HashSet<>());
        records.forEach(i->{
            subscriberIds.add(i.getSubscriberId());
            providerIds.add(i.getProviderId());
            providerLocationIds.add(i.getProviderLocationId());
            fulfillmentIds.add(i.getFulfillmentId());
            categoryIds.add(i.getCategoryId());
            paymentIds.add(i.getPaymentId());
            appItemMap.get(i.getSubscriberId()).put(i.getId(),i);
        });
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Provider>> appProviderMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Provider.class,providerIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.ProviderLocation>> appLocationMap = createAppDbCache(in.succinct.catalog.indexer.db.model.ProviderLocation.class,providerLocationIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Fulfillment>> appFulfillmentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Fulfillment.class,fulfillmentIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Category>> appCategoryMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Category.class,categoryIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Payment>> appPaymentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Payment.class,paymentIds);



        for (String subscriberId : subscriberIds) {

            Map<Long, in.succinct.catalog.indexer.db.model.Item> itemMap = appItemMap.get(subscriberId);
            for (Long itemId : itemMap.keySet()) {
                Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Looping through result items" + itemId);
                in.succinct.catalog.indexer.db.model.Item dbItem = itemMap.get(itemId);

                in.succinct.catalog.indexer.db.model.Provider dbProvider = appProviderMap.get(subscriberId).get(dbItem.getProviderId());
                ProviderLocation dbProviderLocation = appLocationMap.get(subscriberId).get(dbItem.getProviderLocationId());
                Fulfillment dbFulfillment = appFulfillmentMap.get(subscriberId).get(dbItem.getFulfillmentId());
                in.succinct.catalog.indexer.db.model.Category dbCategory = appCategoryMap.get(subscriberId).get(dbItem.getCategoryId());
                Payment dbPayment = appPaymentMap.get(subscriberId).get(dbItem.getPaymentId());

                Provider outProvider = providers.get(dbProvider.getObjectId());
                if (outProvider == null) {
                    outProvider = new Provider(dbProvider.getObjectJson());
                    Time time = new Time();
                    time.setLabel("enable");
                    time.setTimestamp(reply.getContext().getTimestamp());
                    outProvider.setTime(time);
                    providers.add(outProvider);
                }
                Categories categories = outProvider.getCategories();
                if (categories == null) {
                    categories = new Categories();
                    outProvider.setCategories(categories);
                }
                if (dbCategory != null) {
                    Category outCategory = categories.get(dbCategory.getObjectId());
                    if (outCategory == null) {
                        categories.add(new Category(dbCategory.getObjectJson()));
                    }
                }

                Locations locations = outProvider.getLocations();
                if (locations == null) {
                    locations = new Locations();
                    outProvider.setLocations(locations);
                }
                if (dbProviderLocation != null) {
                    Location outLocation = locations.get(dbProviderLocation.getObjectId());
                    if (outLocation == null) {
                        locations.add(new Location(dbProviderLocation.getObjectJson()));
                    }
                }

                Fulfillments fulfillments = outProvider.getFulfillments();
                if (fulfillments == null) {
                    fulfillments = new Fulfillments();
                    outProvider.setFulfillments(fulfillments);
                }
                if (dbFulfillment != null) {
                    in.succinct.beckn.Fulfillment outFulfillment = fulfillments.get(dbFulfillment.getObjectId());
                    if (outFulfillment == null) {
                        outFulfillment = new in.succinct.beckn.Fulfillment(dbFulfillment.getObjectJson());
                        fulfillments.add(outFulfillment);

                        in.succinct.beckn.Fulfillment catFulfillment = catalog.getFulfillments().get(dbFulfillment.getObjectId());
                        if (catFulfillment == null){
                            catFulfillment = new in.succinct.beckn.Fulfillment();
                            catFulfillment.setId(outFulfillment.getId());
                            catFulfillment.setType(outFulfillment.getType());
                            catalog.getFulfillments().add(catFulfillment);
                        }
                    }
                }

                Payments payments = outProvider.getPayments();
                if (payments == null) {
                    payments = new Payments();
                    outProvider.setPayments(payments);
                }
                in.succinct.beckn.Payment bapPaymentIntent = request.getMessage().getIntent().getPayment();
                if (dbPayment != null) {
                    if (payments.get(dbPayment.getObjectId()) == null) {
                        in.succinct.beckn.Payment payment = new in.succinct.beckn.Payment(dbPayment.getObjectJson());
                        payments.add(payment);
                    }
                }
                Items items = outProvider.getItems();
                if (items == null) {
                    items = new Items();
                    outProvider.setItems(items);
                }
                if (items.get(dbItem.getObjectId()) == null) {
                    Item outItem = new Item((JSONObject) JSONAwareWrapper.parse(dbItem.getObjectJson()));
                    if (!outItem.getFulfillmentIds().isEmpty() ) {
                        outItem.setFulfillmentId(outItem.getFulfillmentIds().get(0));
                    }
                    if (!outItem.getLocationIds().isEmpty()) {
                        outItem.setLocationId(outItem.getLocationIds().get(0));
                    }
                    outItem.setTime(new Time());
                    outItem.getTime().setLabel(dbItem.isActive() ? "enable" : "disable");
                    outItem.getTime().setTimestamp(dbItem.getUpdatedAt());
                    FulfillmentType outFulfillmentType = fulfillments.get(outItem.getFulfillmentId()).getType();
                    FulfillmentType inFulfillmentType = intentFulfillment == null ? null : intentFulfillment.getType();
                    FulfillmentStop end = intentFulfillment == null ? null : intentFulfillment.getEnd();

                    if (outFulfillmentType.matches(inFulfillmentType) ) {

                        outItem.setMatched(true);
                        outItem.setRelated(true);
                        outItem.setRecommended(true);

                        ItemQuantity itemQuantity = new ItemQuantity();
                        Quantity available =new Quantity() ;
                        available.setCount(Integer.MAX_VALUE); // Ordering more than 20 is not allowed.
                        itemQuantity.setAvailable(available);
                        itemQuantity.setMaximum(available);
                        outItem.setItemQuantity(itemQuantity);

                        Location storeLocation = locations.get(outItem.getLocationId());
                        City city = City.findByCountryAndStateAndName(storeLocation.getAddress().getCountry(),storeLocation.getAddress().getState(),storeLocation.getAddress().getCity());

                        boolean storeInCity = ObjectUtil.equals(city.getCode(),request.getContext().getCity()) || ObjectUtil.equals(request.getContext().getCity(),"*");
                        boolean includeItem = false;

                        if (end != null && end.getLocation() != null && end.getLocation().getGps() != null){
                            Circle circle = storeLocation.getCircle();
                            includeItem = true;
                            if (circle != null){
                                if (circle.getGps() == null){
                                    circle.setGps(storeLocation.getGps());
                                }
                                if (circle.getGps() != null && circle.getRadius() != null && circle.getRadius().getValue() > 0){
                                    Scalar radius = circle.getRadius();
                                    if (ObjectUtil.isVoid(radius.getUnit())){
                                        radius.setUnit("km");
                                    }
                                    double distance = radius.getValue();
                                    if (!radius.getUnit().equalsIgnoreCase("km")){
                                        distance = convertDistanceToKm(distance,radius.getUnit());
                                    }
                                    if (circle.getGps().distanceTo(end.getLocation().getGps()) > distance){
                                        includeItem = false;
                                    }
                                }
                            }
                        }else if (end == null && storeInCity) {
                            includeItem = true;
                        }
                        if (includeItem){
                            items.add(outItem);
                            numItemsReturned.increment();
                        }
                    }
                }

            }
        }
        reply.setSuppressed(numItemsReturned.intValue() == 0); // No need to send on_search back!T
        catalog.getDescriptor().setName(getSubscriber().getSubscriberId());
        catalog.getDescriptor().setCode(subscriber.getSubscriberId());
        catalog.getDescriptor().setImages(new Images());

    }

    Map<String,Double> conversionFactor = new HashMap<>(){{
       put("km",1.0);
       put("mile",1.609344);
    }};
    private double convertDistanceToKm(double distance, String fromUnit) {
        double f = conversionFactor.get(StringUtil.singularize(fromUnit).toLowerCase());
        return f * distance;
    }

    private Descriptor normalizeDescriptor(Descriptor descriptor) {
        if (descriptor != null && descriptor.getInner().isEmpty()){
            descriptor = null;
        }
        return descriptor;
    }

    private <T extends Model> Cache<Long,T> createDbCache(Class<T> clazz, Set<Long> ids) {
        Cache<Long,T> cache = new Cache<>(0,0){

            @Override
            protected T getValue(Long id) {
                if (id == null){
                    return null;
                }else {
                    return Database.getTable(clazz).get(id);
                }
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.put(t.getId(),t));
        }
        return cache;
    }

    private <T extends Model & IndexedSubscriberModel> Cache<String,Cache<Long,T>> createAppDbCache(Class<T> clazz, Set<Long> ids){
        Cache<String,Cache<Long,T>> cache = new Cache<>(0,0) {
            @Override
            protected Cache<Long, T> getValue(String subscriberId) {
                return createDbCache(clazz,new HashSet<>());
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)){
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(),"ID",Operator.IN,ids.toArray()));
            select.execute(clazz).forEach(t->cache.get(t.getSubscriberId()).put(t.getId(),t));
        }
        return cache;
    }

    private in.succinct.catalog.indexer.db.model.Item getItem(String objectId) {

        Select select = new Select().from(in.succinct.catalog.indexer.db.model.Item.class);
        List<in.succinct.catalog.indexer.db.model.Item> dbItems = select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "SUBSCRIBER_ID", Operator.EQ, getSubscriber().getSubscriberId())).
                add(new Expression(select.getPool(), "OBJECT_ID", Operator.EQ, objectId))).execute(1);

        return dbItems.isEmpty() ? null : dbItems.get(0);
    }

}
