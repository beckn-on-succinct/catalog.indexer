package in.succinct.catalog.indexer.ingest;

import com.venky.cache.Cache;
import com.venky.cache.UnboundedCache;
import com.venky.core.string.StringUtil;
import com.venky.core.util.ObjectHolder;
import com.venky.core.util.ObjectUtil;
import com.venky.geo.GeoCoordinate;
import com.venky.swf.db.Database;
import com.venky.swf.db.model.Model;
import com.venky.swf.db.model.reflection.ModelReflector;
import com.venky.swf.plugins.collab.db.model.config.City;
import com.venky.swf.plugins.collab.util.BoundingBox;
import com.venky.swf.plugins.lucene.index.LuceneIndexer;
import com.venky.swf.routing.Config;
import com.venky.swf.sql.Conjunction;
import com.venky.swf.sql.Expression;
import com.venky.swf.sql.Operator;
import com.venky.swf.sql.Select;
import in.succinct.beckn.BecknAware;
import in.succinct.beckn.BecknStrings;
import in.succinct.beckn.Catalog;
import in.succinct.beckn.Categories;
import in.succinct.beckn.Category;
import in.succinct.beckn.Circle;
import in.succinct.beckn.Context;
import in.succinct.beckn.Descriptor;
import in.succinct.beckn.Fulfillment;
import in.succinct.beckn.Fulfillment.RetailFulfillmentType;
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
import in.succinct.beckn.Payment;
import in.succinct.beckn.Payments;
import in.succinct.beckn.Provider;
import in.succinct.beckn.Providers;
import in.succinct.beckn.Quantity;
import in.succinct.beckn.Request;
import in.succinct.beckn.Scalar;
import in.succinct.beckn.Subscriber;
import in.succinct.beckn.TagGroup;
import in.succinct.beckn.TagGroups;
import in.succinct.beckn.Time;
import in.succinct.catalog.indexer.db.model.IndexedProviderModel;
import in.succinct.catalog.indexer.db.model.IndexedSubscriberModel;
import in.succinct.catalog.indexer.db.model.ProviderLocation;
import in.succinct.catalog.indexer.db.model.ProviderTag;
import org.apache.lucene.search.Query;
import org.bouncycastle.util.Arrays;
import org.json.simple.JSONArray;
import org.json.simple.JSONAware;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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

    public <T extends Model & IndexedSubscriberModel> List<Long> ids(Class<T> modelClazz, String conditionColumnName, String conditionColumnValue, boolean exact, int maxRecords){
        LuceneIndexer indexer = LuceneIndexer.instance(modelClazz);
        StringBuilder q = q(conditionColumnName,conditionColumnValue,exact);
        if (!q.isEmpty()){
            q.insert(0,"(");
            q.append(")");
            StringBuilder subscriberQ = new StringBuilder();
            for (String subcriberId : subscriberMap.keySet()) {
                subscriberQ.append(" ").append(subcriberId).append(" ");
            }
            if (!subscriberQ.isEmpty()){
                q.append( " AND (").append(q("SUBSCRIBER_ID",subscriberQ.toString(),true)).append(" )");
            }
            q.insert(0,"(");
            q.append(")");
            Query query = indexer.constructQuery(q(conditionColumnName,conditionColumnValue,exact).toString());
            return indexer.findIds(query,maxRecords);
        }
        return new ArrayList<>();
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
    public IntentMeta getProviderQuery(Request request,
                                   ObjectHolder<Boolean> providerSpecific ,
                                   ObjectHolder<String> environment,
                                   ObjectHolder<TagGroups> providerTags,Conjunction conjunction) {
        
        IntentMeta intentMeta = new IntentMeta();
        intentMeta.setConjunction(conjunction);
        intentMeta.add(in.succinct.catalog.indexer.db.model.Provider.class,"ID","ID");
        intentMeta.add(in.succinct.catalog.indexer.db.model.Provider.class,"NAME","OBJECT_NAME");
        List<Class<? extends Model>> list = List.of(in.succinct.catalog.indexer.db.model.Item.class,in.succinct.catalog.indexer.db.model.Category.class, ProviderLocation.class, in.succinct.catalog.indexer.db.model.Fulfillment.class, in.succinct.catalog.indexer.db.model.Payment.class, ProviderTag.class);
        
        for (Class<? extends Model> clazz: list){
            intentMeta.add(clazz,"ID","PROVIDER_ID");
            intentMeta.add(clazz,"NAME","PROVIDER");
        }
        
        
        
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();
        
        Provider provider = intent == null ? null : intent.getProvider();
        Descriptor providerDescriptor = provider == null ? null : normalizeDescriptor(provider.getDescriptor());

        String env = provider == null? null : provider.getTag("network","environment");
        environment.set(env);
        TagGroups tags = provider == null ? null : provider.getTags();
        providerTags.set(tags);

        if (conjunction == Conjunction.OR){
            addDescriptorMeta(intentDescriptor,intentMeta);
        }else {
            if (provider != null && !ObjectUtil.isVoid(provider.getId())) {
                providerSpecific.set(true);
                provider.setId(provider.getId().trim());
                List<Long> ids = ids(in.succinct.catalog.indexer.db.model.Provider.class,"OBJECT_ID",provider.getId(),true,1);
                if (!ids.isEmpty()) {
                    intentMeta.add("ID", "%%s:%s".formatted(ids.get(0)));
                }else {
                    intentMeta.add("ID", "%s:NULL");
                }
            }else if (tags != null && !tags.isEmpty()){
                Select select  = new Select().from(ProviderTag.class);
                select.where(new Expression(select.getPool(),Conjunction.AND){{
                    for (TagGroup tg :tags){
                        add(new Expression(select.getPool(),"TAG_GROUP_CODE", Operator.EQ,tg.getId()));
                        
                        TagGroups list = tg.getList();
                        if (list != null && !list.isEmpty()){
                            add(new Expression(select.getPool(),Conjunction.OR){{
                                for (TagGroup option : list){
                                    add (new Expression(select.getPool(),Conjunction.AND){{
                                        add(new Expression(select.getPool(),"TAG_CODE", Operator.EQ,option.getId()));
                                        if (!ObjectUtil.isVoid(option.getValue())) {
                                            add(new Expression(select.getPool(), "TAG_VALUE", Operator.EQ, option.getValue()));
                                        }
                                    }});
                                }
                            }});
                        }
                    }
                }});
                StringBuilder providers = new StringBuilder();
                for (ProviderTag providerTag : select.execute(ProviderTag.class)) {
                    providers.append(providerTag.getProviderId()).append(" ");
                }
                intentMeta.add("ID",q("%s", providers.toString(),true).toString());
            }
            addDescriptorMeta(providerDescriptor,intentMeta);
        }
        return intentMeta;
    }
    public void addDescriptorMeta(Descriptor descriptor , IntentMeta intentMeta){
        if (descriptor != null) {
            String desc = getDescription(descriptor);
            intentMeta.add("NAME", q("%s", desc,false).toString());
        }
    }

    public IntentMeta getCategoryQuery(Request request, Conjunction conjunction) {
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();

        Category category = intent == null ? null : intent.getCategory();
        Descriptor categoryDescriptor = category == null ? null : normalizeDescriptor(category.getDescriptor());

        IntentMeta categoryQ = new IntentMeta();
        categoryQ.setConjunction(conjunction);
        categoryQ.add(in.succinct.catalog.indexer.db.model.Item.class,"ID","CATEGORY_IDS");
        categoryQ.add(in.succinct.catalog.indexer.db.model.Item.class,"NAME","CATEGORY_IDS");
        
        categoryQ.add(in.succinct.catalog.indexer.db.model.Category.class,"ID","OBJECT_ID");
        categoryQ.add(in.succinct.catalog.indexer.db.model.Category.class,"NAME","OBJECT_NAME");
        
        if (conjunction == Conjunction.OR){
            addDescriptorMeta(intentDescriptor,categoryQ);
        }else {
            if (category != null && !ObjectUtil.isVoid(category.getId())) {
                category.setId(category.getId().trim());
                categoryQ.add("ID",String.format(" %%s:%s",category.getId()));
            }
            addDescriptorMeta(categoryDescriptor,categoryQ);
        }
        
        return categoryQ;
    }
    public IntentMeta getFulfillmentQuery(Request request){
        IntentMeta fulfillmentQ = new IntentMeta();
        fulfillmentQ.add(in.succinct.catalog.indexer.db.model.Fulfillment.class,"ID","OBJECT_ID");
        fulfillmentQ.add(in.succinct.catalog.indexer.db.model.Item.class,"ID","FULFILLMENT_IDS");
        
        Intent intent = request.getMessage().getIntent();
        if (intent.getFulfillment() != null && !ObjectUtil.isVoid(intent.getFulfillment().getType())){
            fulfillmentQ.add("ID"," %%s:%s ".formatted(intent.getFulfillment().getType()));
        }
        return fulfillmentQ;
    }
    
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    public IntentMeta getLocationQuery(Request request){
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();

        Location location = intent == null ? null : intent.getLocation();
        GeoCoordinate coordinate = location == null ? null : location.getGps();

        IntentMeta locationQ = new IntentMeta();
        
        locationQ.add(in.succinct.catalog.indexer.db.model.ProviderLocation.class,"LOCATION_ID","OBJECT_ID");
        
        locationQ.add(in.succinct.catalog.indexer.db.model.Provider.class,"PROVIDER_ID","ID");
        locationQ.add(in.succinct.catalog.indexer.db.model.Category.class,"PROVIDER_ID","PROVIDER_ID");
        locationQ.add(in.succinct.catalog.indexer.db.model.Payment.class,"PROVIDER_ID","PROVIDER_ID");
        locationQ.add(in.succinct.catalog.indexer.db.model.Fulfillment.class,"PROVIDER_ID","PROVIDER_ID");
        
        locationQ.add(in.succinct.catalog.indexer.db.model.Item.class,"PROVIDER_ID","PROVIDER_ID");
        locationQ.add(in.succinct.catalog.indexer.db.model.Item.class,"LOCATION_ID","LOCATION_IDS");
        
        
        Set<Long> providerIds = new HashSet<>();
        
        
        if (coordinate != null){
            Select home_delivery_select = new Select().from(ProviderLocation.class);

            home_delivery_select.where(new Expression(home_delivery_select.getPool(),Conjunction.AND).
                                add(new Expression(home_delivery_select.getPool(),"MIN_LAT" , Operator.LE,coordinate.getLat())).
                                add(new Expression(home_delivery_select.getPool(),"MAX_LAT" , Operator.GE,coordinate.getLat())).
                                add(new Expression(home_delivery_select.getPool(),"MIN_LNG" , Operator.LE,coordinate.getLng())).
                                add(new Expression(home_delivery_select.getPool(),"MAX_LNG" , Operator.GE,coordinate.getLng()))
            );
            List<ProviderLocation> providerLocations = home_delivery_select.execute();
            Set<String> locationIds = new HashSet<>();
            for (ProviderLocation providerLocation : providerLocations) {
                locationIds.add(providerLocation.getObjectId());
                providerIds.add(providerLocation.getProviderId());
            }

            BoundingBox bb  = new BoundingBox(coordinate,1,10); //Hardcoded buyer geo fence 10 kms
            List<ProviderLocation> buyerGeoFenceBasedList = bb.find(ProviderLocation.class,0);

            Map<Long,Set<String>> closeByProviderLocationMap = new UnboundedCache<>() {
                @Override
                protected Set<String> getValue(Long key) {
                    return new HashSet<>();
                }
            };
            for (ProviderLocation providerLocation : buyerGeoFenceBasedList) {
                closeByProviderLocationMap.get(providerLocation.getProviderId()).add(providerLocation.getObjectId());
            }

            if (!closeByProviderLocationMap.isEmpty()) {
                Select providerSupportingStorePickup = new Select().from(in.succinct.catalog.indexer.db.model.Fulfillment.class);
                providerSupportingStorePickup.where(new Expression(providerSupportingStorePickup.getPool(),Conjunction.AND).
                        add(new Expression(providerSupportingStorePickup.getPool(), "OBJECT_ID", Operator.EQ, RetailFulfillmentType.store_pickup.toString())).
                        add(new Expression(providerSupportingStorePickup.getPool(), "PROVIDER_ID", Operator.IN, closeByProviderLocationMap.keySet().toArray())));

                Set<Long> storePickupSupportingProviderIds = new HashSet<>();
                for (in.succinct.catalog.indexer.db.model.Fulfillment f : providerSupportingStorePickup.execute(in.succinct.catalog.indexer.db.model.Fulfillment.class)){
                    storePickupSupportingProviderIds.add(f.getProviderId());
                    providerIds.add(f.getProviderId());
                }
                for (Map.Entry<Long,Set<String>> e : closeByProviderLocationMap.entrySet()){
                    if (storePickupSupportingProviderIds.contains(e.getKey())){
                        locationIds.addAll(e.getValue());
                    }
                }
            }
            
            if (!locationIds.isEmpty()) {
                StringBuilder ids = new StringBuilder();
                locationIds.forEach(id-> ids.append(" ").append(id).append(" "));
                locationQ.add("LOCATION_ID",q("%s", ids.toString(),true).toString());
            }else {
                locationQ.add("LOCATION_ID","%s:NULL");
            }
            if (!providerIds.isEmpty()){
                StringBuilder ids = new StringBuilder();
                providerIds.forEach(id-> ids.append(" ").append(id).append(" "));
                locationQ.add("PROVIDER_ID",q("%s", ids.toString(),true).toString());
            }else {
                locationQ.add("PROVIDER_ID","%s:NULL");
            }
        }
        return locationQ;
    }

    public IntentMeta getItemQuery(Request request , Conjunction conjunction) {
        IntentMeta intentMeta = new IntentMeta();
        intentMeta.setConjunction(conjunction);
        
        intentMeta.add( in.succinct.catalog.indexer.db.model.Item.class,"ID","OBJECT_ID");
        intentMeta.add(in.succinct.catalog.indexer.db.model.Item.class,"NAME","OBJECT_NAME");
        intentMeta.add(in.succinct.catalog.indexer.db.model.Item.class,"DOMAIN","DOMAIN");
        
        
        Intent intent = request.getMessage().getIntent();
        Descriptor intentDescriptor = intent == null ? null : intent.getDescriptor();

        Item item = intent == null ? null : intent.getItem();
        Descriptor itemDescriptor = item == null ? null : normalizeDescriptor(item.getDescriptor());
        
        if (conjunction == Conjunction.OR){
            addDescriptorMeta(intentDescriptor,intentMeta);
        }else {
            if (item != null && !ObjectUtil.isVoid(item.getId())) {
                item.setId(item.getId().trim());
                intentMeta.add("ID", "%%s:%s".formatted(item.getId()));
            }
            addDescriptorMeta(itemDescriptor,intentMeta);
            if (!ObjectUtil.isVoid(request.getContext().getDomain())) {
                intentMeta.add("DOMAIN", "%%s:\"%s\"".formatted(request.getContext().getDomain()));
            }
        }


        return intentMeta;
    }

    
    public static class IntentMeta {
        public IntentMeta(){
        }
        Conjunction conjunction = Conjunction.AND ;
        
        public void setConjunction(Conjunction conjunction) {
            this.conjunction = conjunction;
        }
        
        Map<String,String> queryMeta = new Cache<>() {
            @Override
            protected String getValue(String s) {
                return null;
            }
        };
        Map<Class<? extends Model>,Map<String,String>> meta = new Cache<>() {
            @Override
            protected Map<String, String> getValue(Class<? extends Model> aClass) {
                return new Cache<>() {
                    @Override
                    protected String getValue(String s) {
                        return null;
                    }
                };
            }
        };
        public <T extends Model> void add(Class<T> modelClass, String fieldMeta, String field){
            meta.get(modelClass).put(fieldMeta,field);
        }
        public void add(String metaFieldName, String query){
            if (!ObjectUtil.isVoid(query)) {
                queryMeta.put(metaFieldName, query);
            }
        }
        public boolean isPresent(){
            return !queryMeta.isEmpty();
        }
        
        public String q(Class<? extends Model> clazz){
            Map<String,String> fieldMap = meta.get(clazz);
            
            StringBuilder q = new StringBuilder();
            queryMeta.forEach((metaFieldName,fq)->{
                if (fieldMap.containsKey(metaFieldName)) {
                    int count = fq.split("%s").length;
                    Object[] fieldNames = new Object[count];
                    Arrays.fill(fieldNames, fieldMap.get(metaFieldName));
                    if (!q.isEmpty()) {
                        q.append(" ").append(conjunction).append(" ");
                    }
                    q.append(fq.formatted(fieldNames));
                }
            });
            if (!q.isEmpty()) {
                q.insert(0, "(");
                q.append(")");
            }
            
            return q.toString();
        }
    
    }
    public <M extends Model & IndexedSubscriberModel> StringBuilder getLuceneQuery(Class<M> clazz,IntentMeta... intentQueries){
        Map<Conjunction,List<IntentMeta>> queries = new UnboundedCache<>() {
            @Override
            protected List<IntentMeta> getValue(Conjunction key) {
                return new ArrayList<>();
            }
        };
        
        
        for (IntentMeta q : intentQueries) {
            if (q.isPresent()) {
                queries.get(q.conjunction).add(q);
            }
        }
        StringBuilder andQ = new StringBuilder();
        for (IntentMeta intentQuery : queries.get(Conjunction.AND)) {
            if (!andQ.isEmpty()){
                andQ.append(" AND ");
            }
            andQ.append(intentQuery.q(clazz));
        }
        if (!andQ.isEmpty()){
            andQ.insert(0,"(");
            andQ.append(")");
        }
        
        StringBuilder orQ = new StringBuilder();
        for (IntentMeta intentQuery : queries.get(Conjunction.OR)) {
            if (!orQ.isEmpty()){
                orQ.append(" OR ");
            }
            orQ.append(intentQuery.q(clazz));
        }
        if (!orQ.isEmpty()){
            orQ.insert(0,"(");
            orQ.append(")");
        }
        
        StringBuilder q = new StringBuilder();
        if (!andQ.isEmpty()){
            q.append(andQ);
        }
        if (!orQ.isEmpty()){
            if (!q.isEmpty()) {
                q.append(" AND ");
            }
            q.append(orQ);
        }
        if (!q.isEmpty()) {
            q.insert(0, "(");
            q.append(")");
        }
        return q;
    }
    public <M extends Model & IndexedSubscriberModel> List<M> getRecords(Class<M> clazz, Request request,
                            ObjectHolder<String> environment, int numRecords,IntentMeta... intentQueries){
        
        StringBuilder q = getLuceneQuery(clazz,intentQueries);
        
        
        List<Long> ids = new ArrayList<>();
        if (!ObjectUtil.isVoid(q.toString())) {
            LuceneIndexer indexer = LuceneIndexer.instance(clazz);
            Query query = indexer.constructQuery(q.toString());
            Config.instance().getLogger(getClass().getName()).info("Searching for /%s/search/".formatted(StringUtil.underscorize(clazz.getSimpleName()).toLowerCase() )+ q);
            ids = indexer.findIds(query, 0);
            Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Query result size: " + ids.size());
        }
        if (ids.isEmpty()) {
            return new ArrayList<>();
        }
        Select sel = new Select().from(clazz);
        
        Expression where = new Expression(sel.getPool(), Conjunction.AND);
        where.add(new Expression(sel.getPool(), "SUBSCRIBER_ID", Operator.IN, subscriberMap.keySet().toArray(new String[]{})));
        where.add(Expression.createExpression(sel.getPool(), "ID", Operator.IN, ids.toArray()));
        sel.where(where);
        
        StringBuilder extra = new StringBuilder();
        ModelReflector<M> ref = ModelReflector.instance(clazz);
        String tableName = ref.getTableName();
        if (ref.getRealFields().contains("PROVIDER_ID")) {
            extra.append(" and not exists (select 1 from provider_tags where provider_id = %s.provider_id and tag_group_code = 'network' and tag_code = 'suspended' and tag_value = 'Y' )".formatted(tableName));
            extra.append(" and not exists (select 1 from provider_tags where provider_id = %s.provider_id and tag_group_code = 'kyc' and tag_code = 'ok' and tag_value = 'N' )".formatted(tableName));
            if (environment.get() != null) {
                extra.append(String.format(" and exists (select 1 from provider_tags where provider_id = %s.provider_id and tag_group_code = 'network' and tag_code = 'environment' and tag_value = '%s' )",
                        tableName,
                        environment.get()));
            }
            if (!ObjectUtil.isVoid(request.getContext().getDomain()) && !ObjectUtil.equals(clazz.getSimpleName(), in.succinct.catalog.indexer.db.model.Item.class.getSimpleName())){
                extra.append(" and exists ( select 1 from items where domain = '%s' and items.provider_id = %s.provider_id".formatted(request.getContext().getDomain(), tableName));
                if (ObjectUtil.equals(clazz.getSimpleName(),ProviderLocation.class.getSimpleName())){
                    extra.append(" and items.location_ids like '%' || ").append("%s.object_id".formatted(tableName)).append(" || '%'");
                }else if (ObjectUtil.equals(clazz.getSimpleName(), in.succinct.catalog.indexer.db.model.Category.class.getSimpleName())){
                    extra.append(" and items.category_ids like '%' || ").append("%s.object_id".formatted(tableName)).append(" || '%'");
                }
                extra.append(")");
            }
        }else if (ObjectUtil.equals(clazz.getSimpleName(), in.succinct.catalog.indexer.db.model.Provider.class.getSimpleName())){
            if (!ObjectUtil.isVoid(request.getContext().getDomain()) ){
                extra.append(" and exists ( select 1 from items where domain = '%s' and items.provider_id = %s.id)".formatted(request.getContext().getDomain(), tableName));
            }
        }
        
        sel.add(extra.toString());
        
        return sel.where(where).execute(clazz, numRecords);
    }
    
    private <T extends JSONAware,B extends BecknAware<T>> B toBeckn(Class<B> clazz, IndexedSubscriberModel model){
        try {
            return clazz.getConstructor(String.class).newInstance(model.getObjectJson());
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }
    
    
    
    
    @SuppressWarnings({"MismatchedQueryAndUpdateOfCollection", "unchecked"})
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
        
        
        ObjectHolder<Boolean> providerSpecific = new ObjectHolder<>(false);// Whether to return all items or not.
        ObjectHolder<String> environment = new ObjectHolder<>(null);
        ObjectHolder<TagGroups> providerTags = new ObjectHolder<>(null);
        
        IntentMeta providerQuery = getProviderQuery(request, providerSpecific,environment,providerTags,Conjunction.AND);
        IntentMeta softProviderQuery = getProviderQuery(request, providerSpecific,environment,providerTags,Conjunction.OR);
        
        IntentMeta locationQuery = providerSpecific.get() ? providerQuery  : getLocationQuery(request);
        
        IntentMeta categoryQuery = getCategoryQuery(request,Conjunction.AND);
        IntentMeta softCategoryQuery = getCategoryQuery(request,Conjunction.OR);
        
        IntentMeta itemQuery = getItemQuery(request,Conjunction.AND);
        IntentMeta softItemQuery = getItemQuery(request,Conjunction.OR);
        IntentMeta fulfillmentQuery = getFulfillmentQuery(request);

        boolean requiredToReturnItems =  (providerSpecific.get()  || itemQuery.isPresent() || categoryQuery.isPresent() || softItemQuery.isPresent() );
        //If any soft query is present all softquery are present. Because all are related to intent.descriptor in a general way and not item.descriptor or provider.descriptor or category.descriptor.
        
        List<in.succinct.catalog.indexer.db.model.Item> itemRecords =
               requiredToReturnItems ?
                getRecords(in.succinct.catalog.indexer.db.model.Item.class,request,environment,providerSpecific.get()  ? 0 : 50,
                    providerQuery,softProviderQuery,
                    locationQuery,
                    categoryQuery,softCategoryQuery,
                    fulfillmentQuery,
                    itemQuery,softItemQuery) :
                new ArrayList<>();
        // Dont get items for market places.
        if (requiredToReturnItems){
            if (itemRecords.isEmpty()){
                replies.addAll(subscriberReplies.values());
                return;
            }else if (!providerQuery.isPresent()){
                Set<Long> providerIds = new HashSet<>();
                for (in.succinct.catalog.indexer.db.model.Item itemRecord : itemRecords) {
                    providerIds.add(itemRecord.getProviderId());
                }
                StringBuilder qPart = new StringBuilder();
                providerIds.forEach(id->{
                    if (!qPart.isEmpty() ){
                        qPart.append(" ");
                    }
                    qPart.append(id);
                });
                providerQuery.add("ID", q("%s", qPart.toString(),true).toString());
                locationQuery.add("PROVIDER_ID", q("%s", qPart.toString(),true).toString());
                //Restrict to providers being returned.
            }
        }
        
        
        List<in.succinct.catalog.indexer.db.model.ProviderLocation> providerLocationRecords = getRecords(in.succinct.catalog.indexer.db.model.ProviderLocation.class,request,environment,0,
                locationQuery);


        List<in.succinct.catalog.indexer.db.model.Provider> providerRecords = getRecords(in.succinct.catalog.indexer.db.model.Provider.class,request,environment,0,
                locationQuery,
                providerQuery);

        List<in.succinct.catalog.indexer.db.model.Fulfillment> fulfillmentRecords = getRecords(in.succinct.catalog.indexer.db.model.Fulfillment.class,request,environment,0,
                providerQuery,
                locationQuery,
                fulfillmentQuery);

        List<in.succinct.catalog.indexer.db.model.Payment> paymentRecords = getRecords(in.succinct.catalog.indexer.db.model.Payment.class,request,environment,0,
                providerQuery,
                locationQuery);
        List<in.succinct.catalog.indexer.db.model.Category> categoryRecords = getRecords(in.succinct.catalog.indexer.db.model.Category.class,request,environment,0,
                providerQuery,
                locationQuery,
                categoryQuery);
        
        
        
        Cache<String, Cache<Long,in.succinct.catalog.indexer.db.model.Provider>> appProviderMap = new Cache<>(0,0) {
            @Override
            protected Cache<Long, in.succinct.catalog.indexer.db.model.Provider> getValue(String subscriberId) {
                return new Cache<>(0,0) {
                    @Override
                    protected in.succinct.catalog.indexer.db.model.Provider getValue(Long id) {
                        return Database.getTable(in.succinct.catalog.indexer.db.model.Provider.class).get(id);
                    }
                };
            }
        };
        Cache<String, Cache<Long,Cache<String, in.succinct.catalog.indexer.db.model.ProviderLocation>>>  appLocationMap = createAppDbCache(in.succinct.catalog.indexer.db.model.ProviderLocation.class, new HashSet<>());
        Cache<String, Cache<Long,Cache<String, in.succinct.catalog.indexer.db.model.Category>>>  appCategoryMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Category.class, new HashSet<>());
        Cache<String, Cache<Long,Cache<String, in.succinct.catalog.indexer.db.model.Item>>>  appItemMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Item.class, new HashSet<>());
        Cache<String, Cache<Long,Cache<String, in.succinct.catalog.indexer.db.model.Fulfillment>>>  appFulfillmentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Fulfillment.class, new HashSet<>());
        Cache<String, Cache<Long,Cache<String, in.succinct.catalog.indexer.db.model.Payment>>>  appPaymentMap = createAppDbCache(in.succinct.catalog.indexer.db.model.Payment.class, new HashSet<>());
        
        Set<String> allSubscriberIds = new HashSet<>(subscriberMap.keySet());
        if (!providerLocationRecords.isEmpty()){
            loadCache(providerLocationRecords,appLocationMap);
            allSubscriberIds.retainAll(appLocationMap.keySet());
        }
        if (!providerRecords.isEmpty()){
            for (in.succinct.catalog.indexer.db.model.Provider providerRecord : providerRecords) {
                appProviderMap.get(providerRecord.getSubscriberId()).put(providerRecord.getId(),providerRecord);
            }
            allSubscriberIds.retainAll(appProviderMap.keySet());
        }
        if (!categoryRecords.isEmpty()){
            loadCache(categoryRecords,appCategoryMap);
            allSubscriberIds.retainAll(appCategoryMap.keySet());
        }
        if (!itemRecords.isEmpty()){
            loadCache(itemRecords,appItemMap);
            allSubscriberIds.retainAll(appItemMap.keySet());
        }
        if (!fulfillmentRecords.isEmpty()){
            loadCache(fulfillmentRecords,appFulfillmentMap);
            allSubscriberIds.retainAll(appFulfillmentMap.keySet());
        }
        if (!paymentRecords.isEmpty()){
            loadCache(paymentRecords,appPaymentMap);
            allSubscriberIds.retainAll(appPaymentMap.keySet());
        }

        
        for (String subscriberId : allSubscriberIds) {
            Request reply = subscriberReplies.get(subscriberId);
            Context context = reply.getContext();
            Catalog catalog = reply.getMessage().getCatalog();
            Providers providers = catalog.getProviders();
            Cache<Long, in.succinct.catalog.indexer.db.model.Provider> providerCache = appProviderMap.get(subscriberId);
            providerCache.forEach((pid,dbProvider)->{
                Provider outProvider = new Provider(dbProvider.getObjectJson());
                boolean providerIsSuspended = dbProvider.getReflector().getJdbcTypeHelper().getTypeRef(boolean.class).getTypeConverter().valueOf(outProvider.getTag("network","suspended"));
                String env = outProvider.getTag("network","environment");
                if (environment.get() != null && env != null && !ObjectUtil.equals(env,environment.get())){
                    return;
                }
                if (providerIsSuspended){
                    return;
                }
                String sKycOk = outProvider.getTag("kyc","ok");
                
                boolean kycOk = sKycOk == null || dbProvider.getReflector().getJdbcTypeHelper().getTypeRef(boolean.class).getTypeConverter().valueOf(sKycOk);
                if (!kycOk){
                    return;
                }
                
                if (!matches(outProvider,providerTags.get())){
                    return;
                }
                //outProvider.getTags() is updated to match input.
                
                Time time = new Time();
                time.setLabel("enable");
                time.setTimestamp(reply.getContext().getTimestamp());
                outProvider.setTime(time);
                providers.add(outProvider);
                Categories categories = outProvider.getCategories(true);
                Locations locations = outProvider.getLocations(true);
                Fulfillments fulfillments =outProvider.getFulfillments(true);
                Payments payments = outProvider.getPayments(true);
                Items items = outProvider.getItems(true);
                
                Cache<String, in.succinct.catalog.indexer.db.model.ProviderLocation> providerLocationMap = appLocationMap.get(subscriberId).get(pid);
                providerLocationMap.forEach((providerLocationId,dbProviderLocation)->{
                    if (locations.get(providerLocationId) == null){
                        locations.add(toBeckn(Location.class,dbProviderLocation));
                    }
                });
                Cache<String, in.succinct.catalog.indexer.db.model.Category> categoryMap = appCategoryMap.get(subscriberId).get(pid);
                categoryMap.forEach((becknCategoryId,dbCategory)->{
                    if (categories.get(becknCategoryId) == null){
                        categories.add(toBeckn(Category.class,dbCategory));
                    }
                });
                Cache<String, in.succinct.catalog.indexer.db.model.Fulfillment> fulfillmentCache = appFulfillmentMap.get(subscriberId).get(pid);
                fulfillmentCache.forEach((becknFulfillmentId,dbFulfillment)->{
                    if (fulfillments.get(becknFulfillmentId) == null){
                        fulfillments.add(toBeckn(Fulfillment.class,dbFulfillment));
                    }
                });
                
                Cache<String, in.succinct.catalog.indexer.db.model.Payment> paymentCache = appPaymentMap.get(subscriberId).get(pid);
                paymentCache.forEach((becknPaymentId,dbPayment)->{
                    if (payments.get(becknPaymentId) == null){
                        payments.add(toBeckn(Payment.class,dbPayment));
                    }
                });
                

                Cache<String, in.succinct.catalog.indexer.db.model.Item> itemMap = appItemMap.get(subscriberId).get(pid);
                for (String becknItemId : itemMap.keySet()) {
                    Config.instance().getLogger(getClass().getName()).info("SearchAdaptor: Looping through result items" + becknItemId);
                    in.succinct.catalog.indexer.db.model.Item dbItem = itemMap.get(becknItemId);
                    
                    JSONArray fulfillmentIds = BecknAware.parse(dbItem.getFulfillmentIds());
                    JSONArray paymentIds = BecknAware.parse(dbItem.getPaymentIds());
                    JSONArray locationIds = BecknAware.parse(dbItem.getLocationIds());
                    JSONArray categoryIds = BecknAware.parse(dbItem.getCategoryIds());
                    
                    if (!locations.isEmpty()) {
                        locationIds.retainAll(locations.ids());
                        if (locationIds.isEmpty()) {
                            continue;
                        }
                    }
                    
                    if (!categories.isEmpty()) {
                        categoryIds.retainAll(categories.ids());
                        if (categoryIds.isEmpty()) {
                            continue;
                        }
                    }
                    
                    Item outItem = toBeckn(Item.class,dbItem);
                    outItem.setLocationIds(new BecknStrings(locationIds));
                    outItem.setPaymentIds(new BecknStrings(paymentIds));
                    outItem.setFulfillmentIds(new BecknStrings(fulfillmentIds));
                    outItem.setCategoryIds(new BecknStrings(categoryIds));
                    outItem.setTag("domain","id",dbItem.getDomain());
                    
                    
                    if (items.get(dbItem.getObjectId()) == null) {
                        outItem.setTime(new Time());
                        outItem.getTime().setLabel(dbItem.isActive() ? "enable" : "disable");
                        outItem.getTime().setTimestamp(dbItem.getUpdatedAt());
                        //String outFulfillmentType = fulfillments.get(outItem.getFulfillmentId()).getType();
                        String inFulfillmentType = intentFulfillment == null ? null : intentFulfillment.getType();
                        boolean requestedFulfillmentTypeSupported = isFulfillmentTypeSupported( inFulfillmentType, fulfillments);
                        
                        
                        FulfillmentStop end = intentFulfillment == null ? (intent.getLocation() == null ? null : new FulfillmentStop() {{
                            setLocation(intent.getLocation());
                        }}) : intentFulfillment._getEnd();
                        
                        if (requestedFulfillmentTypeSupported) {
                            
                            outItem.setMatched(true);
                            outItem.setRelated(true);
                            outItem.setRecommended(true);
                            
                            ItemQuantity itemQuantity = new ItemQuantity();
                            Quantity available = new Quantity();
                            available.setCount(Integer.MAX_VALUE); // Ordering more than 20 is not allowed.
                            itemQuantity.setAvailable(available);
                            itemQuantity.setMaximum(available);
                            outItem.setItemQuantity(itemQuantity);
                            
                            for (String locationId : outItem.getLocationIds()){
                                
                                Location storeLocation = locations.get(locationId);
                                City city = City.findByCountryAndStateAndName(storeLocation.getAddress().getCountry(), storeLocation.getAddress().getState(), storeLocation.getAddress().getCity());
                                
                                boolean storeInCity = ObjectUtil.equals(city.getCode(), request.getContext().getCity()) || ObjectUtil.equals(request.getContext().getCity(), "*") || ObjectUtil.isVoid(request.getContext().getCity());
                                boolean includeItem = false;
                                
                                
                                if (end != null && end.getLocation() != null && end.getLocation().getGps() != null) {
                                    for (String fId: outItem.getFulfillmentIds()) {
                                        in.succinct.beckn.Fulfillment fulfillment = fulfillments.get(fId);
                                        if (RetailFulfillmentType.valueOf(fulfillment.getType()) == RetailFulfillmentType.store_pickup) {
                                            includeItem = true;
                                        } else if (RetailFulfillmentType.valueOf(fulfillment.getType()) == RetailFulfillmentType.home_delivery){
                                            Circle circle = storeLocation.getCircle();
                                            if (providerSpecific.get() || circle == null) {
                                                includeItem = true;
                                            } else {
                                                if (circle.getGps() == null) {
                                                    circle.setGps(storeLocation.getGps());
                                                }
                                                Scalar radius = circle.getRadius();
                                                
                                                if (radius == null) {
                                                    radius = new Scalar() {{
                                                        setValue(0);
                                                    }};
                                                    circle.setRadius(radius);
                                                }
                                                if (ObjectUtil.isVoid(radius.getUnit())) {
                                                    radius.setUnit("km");
                                                }
                                                double radiusValue = radius.getValue();
                                                if (!radius.getUnit().equalsIgnoreCase("km")) {
                                                    radiusValue = convertDistanceToKm(radiusValue, radius.getUnit());
                                                }
                                                double distance = circle.getGps().distanceTo(end.getLocation().getGps());
                                                if (distance == 0) {
                                                    includeItem = true; //This is to show inactive items for sellers who query by store location.
                                                } else if (distance <= radiusValue) {
                                                    includeItem = dbItem.isActive();
                                                }
                                            }
                                        }
                                        if (includeItem){
                                            break;
                                        }
                                    }
                                    
                                } else if (end == null && storeInCity) {
                                    includeItem = true;
                                }
                                if (includeItem ) {
                                    if (items.get(outItem.getId()) == null) {
                                        items.add(outItem);
                                        reply.setSuppressed(false);
                                    }
                                }else {
                                    outItem.getLocationIds().remove(locationId);
                                }
                            }
                            
                            
                        }
                    }
                }
                
                
            });

            
        }
        replies.addAll(subscriberReplies.values());
    }
    private boolean matches(Provider outProvider, TagGroups inTagGroups){
        TagGroups outTagGroups = outProvider.getTags();
        TagGroups finalGroups = new TagGroups();
        
        if (inTagGroups == null || inTagGroups.isEmpty()){
            TagGroups tagGroups  = outProvider.getTags();
            for (TagGroup tagGroup : tagGroups){
                if (!tagGroup.isDisplay()){
                    tagGroups.remove(tagGroup);
                }
            }
            return true;
        }else if (outTagGroups == null){
            return false;
        }else {
            for (TagGroup inGroup : inTagGroups) {
                TagGroup outGroup = outTagGroups.get(inGroup.getId());
                if (outGroup == null){
                    return false;
                }else if (!inGroup.getList().isEmpty()){
                    for (TagGroup inTag : inGroup.getList()) {
                        TagGroup outTag = outGroup.getList().get(inTag.getId());
                        if (outTag == null){
                            return false;
                        }
                        String inValue = inTag.getValue();
                        String outValue = outTag.getValue();
                        if (!ObjectUtil.isVoid(inValue)){
                            if (!ObjectUtil.equals(inValue,outValue) && !Pattern.matches(inValue,StringUtil.valueOf(outValue))){
                                return false;
                            }
                        }
                        finalGroups.setTag(outGroup.getId(),outTag.getId(),outValue);
                    }
                }else {
                    finalGroups.add(new TagGroup(outGroup.toString()));
                }
            }
            for (TagGroup outGroup : outTagGroups){
                TagGroup finalTagGroup = finalGroups.get(outGroup.getId());
                if (finalTagGroup == null){
                    finalGroups.add(new TagGroup(outGroup.toString()));
                }
            }
        }
        outTagGroups.setInner(finalGroups.getInner());
        outProvider.setTags(outTagGroups);
        return true;
    }
    private boolean isFulfillmentTypeSupported(String inFulfillmentType, Fulfillments fulfillments) {
        boolean supported = true;

        if (inFulfillmentType != null) {
            supported = false;
            for (in.succinct.beckn.Fulfillment fulfillment : fulfillments) {
                if (ObjectUtil.equals(fulfillment.getType(), inFulfillmentType)) {
                    supported = true ;
                    break;
                }
            }
        }
        return supported;
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

    private <T extends Model & IndexedProviderModel> Cache<String, T> createDbCache(Class<T> clazz, String subscriberId , Long providerId,Set<String> ids) {
        Cache<String, T> cache = new Cache<>(0, 0) {

            @Override
            protected T getValue(String id) {
                if (id == null) {
                    return null;
                } else {
                    T m  = Database.getTable(clazz).newRecord();
                    m.setObjectId(id);
                    m.setSubscriberId(subscriberId);
                    m.setProviderId(providerId);
                    m = Database.getTable(clazz).getRefreshed(m);
                    return m;
                }
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)) {
            Select select = new Select().from(clazz);

            Expression where  = new Expression(select.getPool(),Conjunction.AND).
                    add(new Expression(select.getPool(), "OBJECT_ID", Operator.IN, ids.toArray())).
                    add(new Expression(select.getPool(), "PROVIDER_ID", Operator.EQ, providerId)).
                    add(new Expression(select.getPool(), "SUBSCRIBER_ID", Operator.EQ, subscriberId));

            select.execute(clazz).forEach(t -> cache.put(t.getObjectId(), t));
        }
        return cache;
    }

    private <T extends Model & IndexedProviderModel> Cache<String, Cache<Long,Cache<String, T>>> createAppDbCache(Class<T> clazz, Set<String> ids) {
        Cache<String, Cache<Long,Cache<String, T>>> cache = new Cache<>(0, 0) {
            @Override
            protected Cache<Long,Cache<String, T>> getValue(String subscriberId) {
                return new Cache<>() {
                    @Override
                    protected Cache<String, T> getValue(Long providerId) {
                        return createDbCache(clazz, subscriberId,providerId, new HashSet<>());
                    }
                };
            }
        };
        if (!ids.isEmpty() && !ids.contains(null)) {
            loadCache(clazz,ids,cache);
        }
        return cache;
    }
    private <T extends Model & IndexedProviderModel> void loadCache(Class<T> clazz , Set<String> ids, Cache<String, Cache<Long,Cache<String, T>>> cache){
        if (!ids.isEmpty() && !ids.contains(null)) {
            Select select = new Select().from(clazz);
            select.where(Expression.createExpression(select.getPool(),"OBJECT_ID",Operator.IN,ids.toArray()));
            loadCache(select.execute(clazz),cache);
        }
    }
    private <T extends Model & IndexedProviderModel> void loadCache(List<T> objects, Cache<String, Cache<Long,Cache<String, T>>> cache){
        objects.forEach(t-> cache.get(t.getSubscriberId()).get(t.getProviderId()).put(t.getObjectId(), t));
    }
    

}
