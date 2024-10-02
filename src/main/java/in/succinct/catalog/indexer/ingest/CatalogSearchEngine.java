package in.succinct.catalog.indexer.ingest;

import com.venky.cache.Cache;
import com.venky.cache.UnboundedCache;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectHolder;
import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.BecknStrings;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;

@SuppressWarnings("unused")
public class CatalogSearchEngine {
    Map<String, Subscriber> subscriberMap;

    public CatalogSearchEngine(Map<String, Subscriber> subscriberMap) {
        this.subscriberMap = subscriberMap;
    }

    public void search(Request request, List<Request> replies) {
        try {
            indexed_search(request, replies);
        } catch (Exception ex) {
            Config.instance().getLogger(getClass().getName()).log(Level.WARNING, "Exception found", ex);
        }
    }
    private StringBuilder q(String name, String value, boolean exact) {
        StringBuilder q = new StringBuilder();
        if (ObjectUtil.isVoid(value)) {
            return q;
        }
        StringTokenizer tokenizer = new StringTokenizer(value);
        q.append("(");
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (exact){
                q.append(name).append(":\"").append(token).append("\"");
            }else {
                q.append(name).append(":").append(token).append("*");
            }
            if (tokenizer.hasMoreTokens()) {
                q.append(" OR ");
            }
        }
        q.append(")");
        return q;
    }

    public interface QueryExtractor<T extends Model & IndexedSubscriberModel> {
        String extractQuery(T record);
    }

    public <T extends Model & IndexedSubscriberModel> String q(Class<T> clazz, QueryExtractor<T> extractor, String conditionColumnName, String conditionColumnValue,boolean exact) {
        // Do Lucene
        LuceneIndexer indexer = LuceneIndexer.instance(clazz);
        Query query = indexer.constructQuery(q(conditionColumnName,conditionColumnValue,exact).toString());
        List<Long> ids = indexer.findIds(query,0);


        Select select = new Select().from(clazz);

        select.where(new Expression(select.getPool(), Conjunction.AND).
                add(new Expression(select.getPool(), "SUBSCRIBER_ID", Operator.IN, subscriberMap.keySet().toArray(new String[]{}))).
                add(new Expression(select.getPool(), "ID", Operator.IN, ids.toArray())));

        List<T> dbObjects = select.execute();

        StringBuilder q = new StringBuilder();

        if (!dbObjects.isEmpty()) {
            q.append("(");
            for (Iterator<T> i = dbObjects.iterator(); i.hasNext(); ) {
                T dbObject = i.next();
                q.append(extractor.extractQuery(dbObject));
                if (i.hasNext()) {
                    q.append(" OR ");
                }
            }
            q.append(")");
        } else {
            //q.append(String.format("( %s: NULL )", idColumnName)); // Query should fail.
            q.append(extractor.extractQuery(null));
        }
        return q.toString();
    }
    private String getDescription(Descriptor descriptor) {
        if (descriptor == null) {
            return "";
        }
        String name = StringUtil.valueOf(descriptor.getName());
        if (!ObjectUtil.isVoid(descriptor.getCode())) {
            name = name + " " + descriptor.getCode();
        }
        if (!ObjectUtil.isVoid(descriptor.getShortDesc())) {
            name = name + " " + descriptor.getShortDesc();
        }
        if (!ObjectUtil.isVoid(descriptor.getLongDesc())) {
            name = name + " " + descriptor.getLongDesc();
        }
        return name.trim();
    }

    public String getProviderQuery(Request request, ObjectHolder<Conjunction> conjunction, ObjectHolder<Boolean> providerSpecific , ObjectHolder<String> environment) {
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();
        conjunction.set(Conjunction.OR);

        Provider provider = intent == null ? null : intent.getProvider();
        Descriptor providerDescriptor = provider == null ? null : normalizeDescriptor(provider.getDescriptor());

        String env = provider == null? null : provider.getTag("network","environment");
        environment.set(env);
        StringBuilder providerQ = new StringBuilder();

        if (provider != null && !ObjectUtil.isVoid(provider.getId())) {
            provider.setId(provider.getId().trim());
            providerQ.append(q(in.succinct.catalog.indexer.db.model.Provider.class, record -> {
                if (record != null) {
                    providerSpecific.set(true);
                    return String.format(" PROVIDER_ID:%d ", record.getId());
                }else {
                    return " PROVIDER_ID:NULL ";
                }

            }, "OBJECT_ID",provider.getId(),true));

            conjunction.set(Conjunction.AND);
        }else if (providerDescriptor != null || intentDescriptor != null) {
            String desc = getDescription(providerDescriptor != null ? providerDescriptor : intentDescriptor);

            providerQ.append(String.format(" ( %s ) ",
                    q("PROVIDER", desc,false)));

            if (providerDescriptor != null) {
                conjunction.set(Conjunction.AND);
            }
        }

        if (providerQ.length() > 0) {
            providerQ.insert(0, "(");
            providerQ.append(")");
        }
        return providerQ.toString();
    }

    public String getCategoryQuery(Request request, ObjectHolder<Conjunction> conjunction) {
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();
        conjunction.set(Conjunction.OR);

        Category category = intent == null ? null : intent.getCategory();
        Descriptor categoryDescriptor = category == null ? null : normalizeDescriptor(category.getDescriptor());

        StringBuilder categoryQ = new StringBuilder();
        if (category != null && !ObjectUtil.isVoid(category.getId())) {
            category.setId(category.getId().trim());
            categoryQ.append(String.format(" CATEGORY_IDS:%s",category.getId()));
            conjunction.set(Conjunction.AND);
        }else if (categoryDescriptor != null || intentDescriptor != null) {
            String desc = getDescription(categoryDescriptor != null ? categoryDescriptor : intentDescriptor);
            categoryQ.append(q(in.succinct.catalog.indexer.db.model.Category.class,record -> {
                if (record != null) {
                    return String.format(" CATEGORY_IDS:\"%s\" ", record.getObjectId());
                }else {
                    return " CATEGORY_IDS:NULL ";
                }
            }, "OBJECT_NAME",desc,false));
            if (categoryDescriptor != null) {
                conjunction.set(Conjunction.AND);
            }
        }
        if (categoryQ.length() > 0) {
            categoryQ.insert(0, "(");
            categoryQ.append(")");
        }
        return categoryQ.toString();
    }
    public String getLocationQuery(Request request){
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();

        Location location = intent == null ? null : intent.getLocation();
        GeoCoordinate coordinate = location == null ? null : location.getGps();

        StringBuilder locationQ = new StringBuilder();
        if (coordinate != null){
            Select select = new Select().from(ProviderLocation.class);
            select.where(new Expression(select.getPool(),Conjunction.AND).
                    add(new Expression(select.getPool(),"MIN_LAT" , Operator.LE,coordinate.getLat())).
                    add(new Expression(select.getPool(),"MAX_LAT" , Operator.GE,coordinate.getLat())).
                    add(new Expression(select.getPool(),"MIN_LNG" , Operator.LE,coordinate.getLng())).
                    add(new Expression(select.getPool(),"MAX_LNG" , Operator.GE,coordinate.getLng())));

            List<ProviderLocation> providerLocations = select.execute();
            if (!providerLocations.isEmpty()) {
                locationQ.append("(");
                for (ProviderLocation pl : providerLocations) {
                    if (locationQ.length() > 1) {
                        locationQ.append(" OR ");
                    }
                    locationQ.append(String.format("LOCATION_IDS:\"%s\"", pl.getObjectId()));
                }
                locationQ.append(")");
            }else {
                locationQ.append("LOCATION_IDS:NULL");
            }
        }
        return locationQ.toString();
    }

    public String getItemQuery(Request request, ObjectHolder<Conjunction> conjunction) {
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();
        conjunction.set(Conjunction.OR);

        Item item = intent == null ? null : intent.getItem();
        Descriptor itemDescriptor = item == null ? null : normalizeDescriptor(item.getDescriptor());

        StringBuilder itemQ = new StringBuilder();
        if (item != null && !ObjectUtil.isVoid(item.getId())) {
            item.setId(item.getId().trim());
            itemQ.append(q("OBJECT_ID", item.getId(),true));
            conjunction.set(Conjunction.AND);
        }else if (itemDescriptor != null || intentDescriptor != null) {
            String desc = getDescription(itemDescriptor != null ? itemDescriptor : intentDescriptor);
            itemQ.append(q("OBJECT_NAME", desc,false));
            if (itemDescriptor != null) {
                conjunction.set(Conjunction.AND);
            }
        }

        if (itemQ.length() > 0) {
            itemQ.insert(0, "(");
            itemQ.append(")");
        }
        return itemQ.toString();
    }

    static class IntentQuery {
        IntentQuery(String query , Conjunction conjunction) {
            this.query = query;
            this.present = !ObjectUtil.isVoid(query);
            this.conjunction = conjunction;
        }
        String query ;
        boolean present;
        Conjunction conjunction;
    }
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private void indexed_search(Request request, List<Request> replies) {

        Map<String, Request> subscriberReplies = new Cache<>(0, 0) {
            @Override
            protected Request getValue(String subscriberId) {
                return new Request() {{
                    setSuppressed(false);
                    setContext(new Context(request.getContext().toString()) {{
                        setAction("on_search");
                        setBppUri(subscriberMap.get(subscriberId).getSubscriberUrl());
                        setBppId(subscriberId);
                    }});
                    setMessage(new Message() {{
                        setCatalog(new Catalog() {{
                            setDescriptor(new Descriptor() {{
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
        subscriberMap.forEach((k, v) -> {
            subscriberReplies.get(k); // load subscriber id
        });
        //request.getContext().

        Message message = request.getMessage();
        Intent intent = message.getIntent();
        if (intent.isIncrementalRequestStartTrigger()){
            intent.setStartTime(request.getContext().getTimestamp());
        } else if (intent.isIncrementalRequestEndTrigger()) {
            intent.setEndTime(request.getContext().getTimestamp());
        }

        in.succinct.beckn.Fulfillment intentFulfillment = intent.getFulfillment();
        if (intent.getDescriptor() != null){
            intent.setDescriptor(normalizeDescriptor(intent.getDescriptor()));
        }

        ObjectHolder<Conjunction> providerConjunction = new ObjectHolder<>(null);
        ObjectHolder<Conjunction> categoryConjunction = new ObjectHolder<>(null);
        ObjectHolder<Conjunction> itemConjunction = new ObjectHolder<>(null);
        ObjectHolder<Boolean> providerSpecific = new ObjectHolder<>(false);
        ObjectHolder<String> environment = new ObjectHolder<>(null);

        List<IntentQuery> list = new ArrayList<>();

        IntentQuery providerQuery = new IntentQuery(getProviderQuery(request, providerConjunction,providerSpecific,environment),providerConjunction.get());
        IntentQuery categoryQuery = new IntentQuery(getCategoryQuery(request, categoryConjunction),categoryConjunction.get());
        IntentQuery itemQuery = new IntentQuery(getItemQuery(request,itemConjunction),itemConjunction.get());
        IntentQuery locationQuery = new IntentQuery(providerSpecific.get() ? ""  : getLocationQuery(request),Conjunction.AND);
        Map<Conjunction,List<IntentQuery>> queries = new UnboundedCache<>() {
            @Override
            protected List<IntentQuery> getValue(Conjunction key) {
                return new ArrayList<>();
            }
        };


        for (IntentQuery q : new IntentQuery[]{providerQuery,locationQuery,categoryQuery,itemQuery}) {
            if (q.present ) {
                queries.get(q.conjunction).add(q);
            }
        }
        StringBuilder q = new StringBuilder();
        for (IntentQuery intentQuery : queries.get(Conjunction.AND)) {
            if (q.length() > 0){
                q.append(" AND ");
            }
            q.append(intentQuery.query);
        }
        StringBuilder orQ = new StringBuilder();
        for (IntentQuery intentQuery : queries.get(Conjunction.OR)) {
            if (orQ.length() > 0){
                orQ.append(" OR ");
            }
            orQ.append(intentQuery.query);
        }

        if (q.length() > 0){
            if (orQ.length() > 0) {
                q.append(" AND (");
                q.append(orQ);
                q.append(")");
            }
        }else {
            q.append(orQ);
        }
        if (q.length() > 0 ){
            q.insert(0,"(");
            q.append(")");
        }

        List<Long> itemIds = new ArrayList<>();
        if (!ObjectUtil.isVoid(q.toString())) {
            LuceneIndexer indexer = LuceneIndexer.instance(in.succinct.catalog.indexer.db.model.Item.class);
            Query query = indexer.constructQuery(q.toString());
            Config.instance().getLogger(getClass().getName()).info("Searching for /items/search/" + q);
            itemIds = indexer.findIds(query, 0);
            Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Query result size: " + itemIds.size());
            if (itemIds.isEmpty()) {
                replies.addAll(subscriberReplies.values()); //Send empty responses.
                return;
            }
        }


        Select sel = new Select().from(in.succinct.catalog.indexer.db.model.Item.class);
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        where.add(new Expression(sel.getPool(), "ACTIVE", Operator.EQ, true));
        where.add(new Expression(sel.getPool(), "SUBSCRIBER_ID", Operator.IN, subscriberMap.keySet().toArray(new String[]{})));

        if (!itemIds.isEmpty()) {
            where.add(Expression.createExpression(sel.getPool(), "ID", Operator.IN, itemIds.toArray()));
        }


        sel.where(where);
        StringBuilder extra = new StringBuilder();
        extra.append(" and not exists (select 1 from provider_tags where provider_id = items.provider_id and tag_group_code = 'network' and tag_code = 'suspended' and tag_value = 'Y' )");
        extra.append(" and not exists (select 1 from provider_tags where provider_id = items.provider_id and tag_group_code = 'kyc' and tag_code = 'complete' and tag_value = 'N' )");
        if (environment.get() != null) {
            extra.append(String.format(" and exists (select 1 from provider_tags where provider_id = items.provider_id and tag_group_code = 'network' and tag_code = 'environment' and tag_value = '%s' )",
                    environment.get()));
        }
        sel.add(extra.toString());

        List<in.succinct.catalog.indexer.db.model.Item> records = sel.where(where).execute(in.succinct.catalog.indexer.db.model.Item.class, subscriberMap.size() == 1 ? Select.MAX_RECORDS_ALL_RECORDS : 100);

        Set<String> allSubscriberIds = new HashSet<>();
        Set<String> allProviderIds = new HashSet<>();
        Set<String> allProviderLocationIds = new HashSet<>();
        Set<String> allFulfillmentIds = new HashSet<>();
        Set<String> allCategoryIds = new HashSet<>();
        Set<String> allPaymentIds = new HashSet<>();

        Cache<String, Cache<String, in.succinct.catalog.indexer.db.model.Item>> appItemMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Item.class, new HashSet<>());
        records.forEach(i -> {
            allSubscriberIds.add(i.getSubscriberId());
            allProviderIds.add(i.getProvider().getObjectId());
            allProviderLocationIds.addAll(BecknStrings.parse(i.getLocationIds()));
            allFulfillmentIds.addAll(BecknStrings.parse(i.getFulfillmentIds()));
            allCategoryIds.addAll(BecknStrings.parse(i.getCategoryIds()));
            allPaymentIds.addAll(BecknStrings.parse(i.getPaymentIds()));
            appItemMap.get(i.getSubscriberId()).put(i.getObjectId(), i);
        });
        Cache<String, Cache<String, in.succinct.catalog.indexer.db.model.Provider>> appProviderMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Provider.class, allProviderIds);
        Cache<String, Cache<String, in.succinct.catalog.indexer.db.model.ProviderLocation>> appLocationMap = createAppDbCache(in.succinct.catalog.indexer.db.model.ProviderLocation.class, allProviderLocationIds);
        Cache<String, Cache<String, in.succinct.catalog.indexer.db.model.Fulfillment>> appFulfillmentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Fulfillment.class, allFulfillmentIds);
        Cache<String, Cache<String, in.succinct.catalog.indexer.db.model.Category>> appCategoryMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Category.class, allCategoryIds);
        Cache<String, Cache<String, in.succinct.catalog.indexer.db.model.Payment>> appPaymentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Payment.class, allPaymentIds);
        Cache<String, Cache<Long, in.succinct.catalog.indexer.db.model.Provider>> altProviderMap = new Cache<>(0,0) {
            @Override
            protected Cache<Long, in.succinct.catalog.indexer.db.model.Provider> getValue(String subscriberId) {
                return new Cache<>() {
                    @Override
                    protected in.succinct.catalog.indexer.db.model.Provider getValue(Long aLong) {
                        return null;
                    }
                };
            }
        };
        appProviderMap.forEach((s,c)-> c.forEach((oid, v) -> altProviderMap.get(s).put(v.getId(), v)));

        for (String subscriberId : allSubscriberIds) {
            Request reply = subscriberReplies.get(subscriberId);
            Context context = reply.getContext();
            Catalog catalog = reply.getMessage().getCatalog();
            Providers providers = catalog.getProviders();

            Map<String, in.succinct.catalog.indexer.db.model.Item> itemMap = appItemMap.get(subscriberId);
            for (String becknItemId : itemMap.keySet()) {
                Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Looping through result items" + becknItemId);
                in.succinct.catalog.indexer.db.model.Item dbItem = itemMap.get(becknItemId);

                in.succinct.catalog.indexer.db.model.Provider dbProvider = altProviderMap.get(subscriberId).get(dbItem.getProviderId());
                BecknStrings categoryIds = new BecknStrings(dbItem.getCategoryIds());
                BecknStrings fulfillmentIds = new BecknStrings(dbItem.getFulfillmentIds());
                BecknStrings paymentIds = new BecknStrings(dbItem.getPaymentIds());
                BecknStrings locationIds = new BecknStrings(dbItem.getLocationIds());

                Provider outProvider = providers.get(dbProvider.getObjectId());
                if (outProvider == null) {
                    outProvider = new Provider(dbProvider.getObjectJson());
                    boolean providerIsSuspended = dbProvider.getReflector().getJdbcTypeHelper().getTypeRef(boolean.class).getTypeConverter().valueOf(outProvider.getTag("network","suspended"));
                    String env = outProvider.getTag("network","environment");
                    if (environment.get() != null && env != null && !ObjectUtil.equals(env,environment.get())){
                        continue;
                    }
                    if (providerIsSuspended){
                        continue;
                    }
                    String sKycComplete = outProvider.getTag("kyc","complete");

                    boolean kycComplete = sKycComplete == null || dbProvider.getReflector().getJdbcTypeHelper().getTypeRef(boolean.class).getTypeConverter().valueOf(sKycComplete);
                    if (!kycComplete){
                        continue;
                    }



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
                for (String categoryId : categoryIds) {
                    if (categories.get(categoryId) == null) {
                        in.succinct.catalog.indexer.db.model.Category category = appCategoryMap.get(subscriberId).get(categoryId);
                        if (category != null) {
                            categories.add(new Category(category.getObjectJson()));
                        }
                    }
                }
                Locations locations = outProvider.getLocations();
                if (locations == null) {
                    locations = new Locations();
                    outProvider.setLocations(locations);
                }
                for (String locationId : locationIds) {
                    if (locations.get(locationId) == null){
                        ProviderLocation providerLocation = appLocationMap.get(subscriberId).get(locationId);
                        if (providerLocation != null) {
                            locations.add(new Location(providerLocation.getObjectJson()));
                        }
                    }
                }
                Fulfillments fulfillments = outProvider.getFulfillments();
                if (fulfillments == null) {
                    fulfillments = new Fulfillments();
                    outProvider.setFulfillments(fulfillments);
                }

                for (String fulfillmentId : fulfillmentIds) {
                    if (fulfillments.get(fulfillmentId) == null){
                        Fulfillment fulfillment = appFulfillmentMap.get(subscriberId).get(fulfillmentId);
                        if (fulfillment != null){
                            in.succinct.beckn.Fulfillment outFulfillment = new in.succinct.beckn.Fulfillment(fulfillment.getObjectJson());
                            fulfillments.add(outFulfillment);
                            in.succinct.beckn.Fulfillment catFulfillment = catalog.getFulfillments().get(fulfillmentId);
                            if (catFulfillment == null){
                                catFulfillment = new in.succinct.beckn.Fulfillment();
                                catFulfillment.setId(fulfillmentId);
                                catFulfillment.setType(outFulfillment.getType());
                                catalog.getFulfillments().add(catFulfillment);
                            }
                        }
                    }
                }
                Payments payments = outProvider.getPayments();
                if (payments == null) {
                    payments = new Payments();
                    outProvider.setPayments(payments);
                }
                for (String paymentId : paymentIds) {
                    if (payments.get(paymentId) == null){
                        Payment payment = appPaymentMap.get(subscriberId).get(paymentId);
                        if (payment != null){
                            payments.add(new in.succinct.beckn.Payment(payment.getObjectJson()));
                        }
                    }
                }

                Items items = outProvider.getItems();
                if (items == null) {
                    items = new Items();
                    outProvider.setItems(items);
                }
                if (items.get(dbItem.getObjectId()) == null) {
                    Item outItem = new Item((JSONObject) JSONAwareWrapper.parse(dbItem.getObjectJson()));
                    if (!outItem.getFulfillmentIds().isEmpty()) {
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
                    FulfillmentStop end = intentFulfillment == null ? (intent.getLocation() == null ? null : new FulfillmentStop() {{
                        setLocation(intent.getLocation());
                    }}) : intentFulfillment.getEnd();

                    if (inFulfillmentType == null || outFulfillmentType.matches(inFulfillmentType)) {

                        outItem.setMatched(true);
                        outItem.setRelated(true);
                        outItem.setRecommended(true);

                        ItemQuantity itemQuantity = new ItemQuantity();
                        Quantity available = new Quantity();
                        available.setCount(Integer.MAX_VALUE); // Ordering more than 20 is not allowed.
                        itemQuantity.setAvailable(available);
                        itemQuantity.setMaximum(available);
                        outItem.setItemQuantity(itemQuantity);

                        Location storeLocation = locations.get(outItem.getLocationId());
                        City city = City.findByCountryAndStateAndName(storeLocation.getAddress().getCountry(), storeLocation.getAddress().getState(), storeLocation.getAddress().getCity());

                        boolean storeInCity = ObjectUtil.equals(city.getCode(), request.getContext().getCity()) || ObjectUtil.equals(request.getContext().getCity(), "*") || ObjectUtil.isVoid(request.getContext().getCity());
                        boolean includeItem = false;


                        if (end != null && end.getLocation() != null && end.getLocation().getGps() != null) {
                            Circle circle = storeLocation.getCircle();
                            includeItem = true;
                            if (circle != null && !providerSpecific.get()) {
                                if (circle.getGps() == null) {
                                    circle.setGps(storeLocation.getGps());
                                }
                                if (circle.getGps() != null && circle.getRadius() != null && circle.getRadius().getValue() > 0) {
                                    Scalar radius = circle.getRadius();
                                    if (ObjectUtil.isVoid(radius.getUnit())) {
                                        radius.setUnit("km");
                                    }
                                    double distance = radius.getValue();
                                    if (!radius.getUnit().equalsIgnoreCase("km")) {
                                        distance = convertDistanceToKm(distance, radius.getUnit());
                                    }
                                    if (circle.getGps().distanceTo(end.getLocation().getGps()) > distance) {
                                        includeItem = false;
                                    }
                                }
                            }
                        } else if (end == null && storeInCity) {
                            includeItem = true;
                        }
                        if (includeItem) {
                            items.add(outItem);
                            reply.setSuppressed(false);
                        }
                    }
                }
            }
        }
        replies.addAll(subscriberReplies.values());
    }


    private double convertDistanceToKm(double distance, String fromUnit) {
        return DistanceUtil.convertDistanceToKm(distance,fromUnit);
    }

    private Descriptor normalizeDescriptor(Descriptor descriptor) {
        if (descriptor != null) {
            if (descriptor.getInner().isEmpty() || ObjectUtil.isVoid(getDescription(descriptor))) {
                descriptor = null;
            }
        }
        return descriptor;
    }

    private <T extends Model & IndexedSubscriberModel> Cache<String, T> createDbCache(Class<T> clazz, String subscriberId ,Set<String> ids) {
        Cache<String, T> cache = new Cache<>(0, 0) {

            @Override
            protected T getValue(String id) {
                if (id == null) {
                    return null;
                } else {
                    T m  = Database.getTable(clazz).newRecord();
                    m.setObjectId(id);
                    m.setSubscriberId(subscriberId);
                    m = Database.getTable(clazz).getRefreshed(m);
                    return m;
                }
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)) {
            Select select = new Select().from(clazz);

            Expression where  = new Expression(select.getPool(),Conjunction.AND).
                    add(new Expression(select.getPool(), "OBJECT_ID", Operator.IN, ids.toArray())).
                    add(new Expression(select.getPool(), "SUBSCRIBER_ID", Operator.EQ, subscriberId));

            select.execute(clazz).forEach(t -> cache.put(t.getObjectId(), t));
        }
        return cache;
    }

    private <T extends Model & IndexedSubscriberModel> Cache<String, Cache<String, T>> createAppDbCache(Class<T> clazz, Set<String> ids) {
        Cache<String, Cache<String, T>> cache = new Cache<>(0, 0) {
            @Override
            protected Cache<String, T> getValue(String subscriberId) {
                return createDbCache(clazz, subscriberId, new HashSet<>());
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)) {
            Select select = new Select().from(clazz);
            select.where(new Expression(select.getPool(), "OBJECT_ID", Operator.IN, ids.toArray()));
            select.execute(clazz).forEach(t -> cache.get(t.getSubscriberId()).put(t.getObjectId(), t));
        }
        return cache;
    }

}
