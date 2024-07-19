package in.succinct.catalog.indexer.ingest;

import com.venky.cache.Cache;
import com.venky.core.string.StringUtil;
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
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;

@SuppressWarnings("unused")
public class CatalogSearchEngine {
    Map<String,Subscriber> subscriberMap;
    public CatalogSearchEngine(Map<String,Subscriber> subscriberMap){
        this.subscriberMap = subscriberMap;
    }
    public void search(Request request, List<Request> replies) {
        try{
            indexed_search(request,replies);
        }catch (Exception ex){
            Config.instance().getLogger(getClass().getName()).log(Level.WARNING,"Exception found", ex);
        }
    }
    private StringBuilder q(String name, String value){
        StringBuilder q = new StringBuilder();
        if (ObjectUtil.isVoid(value)){
            return q;
        }
        StringTokenizer tokenizer = new StringTokenizer(value);
        q.append("(");
        while (tokenizer.hasMoreTokens()){
            String token = tokenizer.nextToken();
            q.append(name).append(":").append(token).append("*");
            if (tokenizer.hasMoreTokens()){
                q.append(" OR ");
            }
        }
        q.append(")");

        return q;
    }
    public <T extends Model & IndexedSubscriberModel> String q (Class<T> clazz, String name, String idValue){
        Select select  = new Select().from(clazz);
        select.where(new Expression(select.getPool(),Conjunction.AND).
                add(new Expression(select.getPool(),"SUBSCRIBER_ID", Operator.IN,subscriberMap.keySet().toArray(new String[]{}))).
                add(new Expression(select.getPool(),"OBJECT_ID", Operator.EQ,idValue)));

        List<T> dbObjects = select.execute();

        StringBuilder q = new StringBuilder();

        if (!dbObjects.isEmpty()){
            q.append("(");
            for (Iterator<T> i = dbObjects.iterator(); i.hasNext(); ){
                T dbObject = i.next();
                q.append(String.format(" %s:%d ", name,dbObject.getId()));
                if (i.hasNext()){
                    q.append(" OR ");
                }
            }
            q.append(")");
        }
        return q.toString();
    }


    private void indexed_search(Request request, List<Request> replies) {

        Map<String,Request> subscriberReplies = new Cache<String, Request>(0,0) {
            @Override
            protected Request getValue(String subscriberId) {
                return new Request(){{
                    setSuppressed(true);
                    setContext(new Context(request.getContext().toString()){{
                        setAction("on_search");
                        setBppUri(subscriberMap.get(subscriberId).getSubscriberUrl());
                        setBppId(subscriberId);
                    }});
                    setMessage(new Message(){{
                        setCatalog(new Catalog(){{
                            setDescriptor(new Descriptor(){{
                                setName(subscriberId);
                                setCode(subscriberId);
                                setImages(new Images());
                            }});
                            setProviders(new Providers());
                            setFulfillments(new Fulfillments());
                        }});
                    }});
                }};
            }
        };
        //request.getContext().

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
            q.append(String.format(" ( %s OR %s )",
                    q("PROVIDER",providerDescriptor.getName()),
                    q("PROVIDER_LOCATION",providerDescriptor.getName())));
        }else if (provider != null && !ObjectUtil.isVoid(provider.getId())) {
            q.append(q(in.succinct.catalog.indexer.db.model.Provider.class,"PROVIDER_ID",provider.getId()));
        }
        if (categoryDescriptor != null && !ObjectUtil.isVoid(categoryDescriptor.getName())){
            categoryDescriptor.setName(categoryDescriptor.getName().trim());
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(q("CATEGORY",categoryDescriptor.getName()));
        }else if (category != null && !ObjectUtil.isVoid(category.getId())){
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            category.setId(category.getId().trim());
            q.append(q(in.succinct.catalog.indexer.db.model.Category.class,"CATEGORY_ID",category.getId()));
        }

        if (itemDescriptor != null && !ObjectUtil.isVoid(itemDescriptor.getName())){
            itemDescriptor.setName(itemDescriptor.getName().trim());
            if (q.length() > 0){
                q.append( intentDescriptor == null ? " AND " : " OR " );
            }
            q.append(q("OBJECT_NAME",itemDescriptor.getName()));
        }
        List<Long> itemIds = new ArrayList<>();
        if (!ObjectUtil.isVoid(q.toString())) {
            LuceneIndexer indexer = LuceneIndexer.instance(in.succinct.catalog.indexer.db.model.Item.class);
            Query query = indexer.constructQuery(q.toString());
            Config.instance().getLogger(getClass().getName()).info("Searching for /items/search/" + q);
            itemIds = indexer.findIds(query, 0);
            Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Query result size: " + itemIds.size());
            if (itemIds.isEmpty()) {
                return;
            }
        }


        Select sel = new Select().from(in.succinct.catalog.indexer.db.model.Item.class);
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        where.add(new Expression(sel.getPool(), "ACTIVE", Operator.EQ, true));
        where.add(new Expression(sel.getPool(),"SUBSCRIBER_ID", Operator.IN, subscriberMap.keySet().toArray(new String[]{})));

        if (!itemIds.isEmpty()){
            where.add(Expression.createExpression(sel.getPool(),"ID",Operator.IN,itemIds.toArray()));
        }

        sel.where(where);

        List<in.succinct.catalog.indexer.db.model.Item> records = sel.where(where).execute(in.succinct.catalog.indexer.db.model.Item.class, 50);

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
            Request reply = subscriberReplies.get(subscriberId);
            Context context = reply.getContext();
            Catalog catalog = reply.getMessage().getCatalog();
            Providers providers = catalog.getProviders();

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
                    String outFulfillmentType = fulfillments.get(outItem.getFulfillmentId()).getType();
                    String inFulfillmentType = intentFulfillment == null ? null : intentFulfillment.getType();
                    FulfillmentStop end = intentFulfillment == null ? (intent.getLocation() == null ? null : new FulfillmentStop(){{
                        setLocation(intent.getLocation());
                    }}) : intentFulfillment.getEnd();

                    if (inFulfillmentType == null || outFulfillmentType.matches(inFulfillmentType) ) {

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

                        boolean storeInCity = ObjectUtil.equals(city.getCode(),request.getContext().getCity()) || ObjectUtil.equals(request.getContext().getCity(),"*") || ObjectUtil.isVoid(request.getContext().getCity());
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
                            reply.setSuppressed(false);
                        }
                    }
                }
            }
        }
        replies.addAll(subscriberReplies.values());
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

}
